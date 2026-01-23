#!/usr/bin/env python
"""
Cortex Research Agent - Retrieves data from Snowflake using Cortex Agent.
"""

import json
import os
from typing import Literal, Type
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command
from langfuse import observe
from pydantic import BaseModel
from snowflake.core import Root
from snowflake.core.cortex.lite_agent_service import AgentRunRequest
from snowflake.snowpark import Session

from .shared import State, llm
from ..prompts import agent_system_prompt

# Load environment variables
load_dotenv(override=True)

# Snowflake configuration
SEMANTIC_MODEL_FILE = f"@{os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.{os.getenv('SNOWFLAKE_SEMANTIC_MODEL_STAGE', 'models')}/mint_semantic_model_2.yaml"


class CortexAgentArgs(BaseModel):
    """Arguments schema for the Cortex Agent tool."""

    query: str


class CortexAgentTool:
    """
    Snowflake Cortex Agent that retrieves data using:
    - Cortex Analyst for Text2SQL conversion on structured data
    """

    name: str = "CortexAgent"
    description: str = "answers questions using data from fct_mint table"
    args_schema: Type[CortexAgentArgs] = CortexAgentArgs

    def __init__(self, session: Session):
        self._session = session
        self._root = Root(session)
        self._agent_service = self._root.cortex_agent_service

    def _build_request(self, query: str) -> AgentRunRequest:
        """Build the agent request with Cortex Analyst tool."""
        return AgentRunRequest.from_dict(
            {
                "model": "claude-3-5-sonnet",
                "tools": [
                    {
                        "tool_spec": {
                            "type": "cortex_analyst_text_to_sql",
                            "name": "analyst1",
                        }
                    },
                ],
                "tool_resources": {
                    "analyst1": {"semantic_model_file": SEMANTIC_MODEL_FILE},
                },
                "messages": [
                    {"role": "user", "content": [{"type": "text", "text": query}]}
                ],
            }
        )

    def _consume_stream(self, stream):
        """Process the streaming response from Cortex Agent."""
        text, sql, citations = "", "", []
        for evt in stream.events():
            try:
                delta = (
                    evt.data.get("delta")
                    if isinstance(evt.data, dict)
                    else json.loads(evt.data).get("delta")
                    or json.loads(evt.data).get("data", {}).get("delta")
                )
            except Exception:
                continue

            if not isinstance(delta, dict):
                continue

            for item in delta.get("content", []):
                if item.get("type") == "text":
                    text += item.get("text", "")
                elif item.get("type") == "tool_results":
                    for result in item["tool_results"].get("content", []):
                        if result.get("type") != "json":
                            continue
                        j = result["json"]
                        text += j.get("text", "")
                        sql = j.get("sql", sql)
                        citations.extend(
                            {"source_id": s.get("source_id"), "doc_id": s.get("doc_id")}
                            for s in j.get("searchResults", [])
                        )
        return text, sql, str(citations)

    @observe(name="cortex_agent_run")
    def run(self, query: str, **kwargs):
        """
        Run the Cortex Agent to retrieve sales-related data from Snowflake.

        Uses both Text2SQL (Cortex Analyst) and Semantic Search (Cortex Search)
        to answer questions about structured CRM data and unstructured meeting notes.
        """
        req = self._build_request(query)
        stream = self._agent_service.run(req)
        text, sql, citations = self._consume_stream(stream)

        results_str = ""
        if sql:
            try:
                # Ensure warehouse is set before running SQL
                self._session.sql(
                    f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}"
                ).collect()
                df = self._session.sql(sql.rstrip(";")).to_pandas()
                results_str = df.to_string(index=False)
            except Exception as e:
                results_str = f"SQL execution error: {e}"

        return text, citations, sql, results_str


# Initialize Cortex Agent
try:
    from scripts.utils.database_client import SnowflakeClient

    cortex_agent_tool = CortexAgentTool(
        session=SnowflakeClient.from_env().snowpark_session()
    )
    cortex_agent = create_react_agent(
        llm,
        tools=[cortex_agent_tool.run],
        prompt=agent_system_prompt(
            """
            You are the Researcher. You can answer questions
            using data from fct_mint table.
            Do not take any further action.
        """
        ),
    )
    CORTEX_AGENT_AVAILABLE = True
except Exception as e:
    print(f"Warning: Cortex Agent not available: {e}")
    cortex_agent = None
    CORTEX_AGENT_AVAILABLE = False


@observe(name="cortex_analyst_node")
def cortex_analyst_node(state: State) -> Command[Literal["executor"]]:
    """
    Cortex research sub-agent node.

    Uses Snowflake Cortex Agent to retrieve sales-related data from Snowflake
    using both Text2SQL (Cortex Analyst) and Semantic Search (Cortex Search).
    """
    if not CORTEX_AGENT_AVAILABLE:
        error_message = (
            "Cortex Agent is not available. Please check Snowflake configuration."
        )
        return Command(
            update={
                "messages": [HumanMessage(content=error_message, name="cortex_analyst")]
            },
            goto="executor",
        )

    query = state.get("agent_query", state.get("user_query", ""))
    agent_response = cortex_agent.invoke({"messages": query})
    new_message = HumanMessage(
        content=agent_response["messages"][-1].content, name="cortex_analyst"
    )
    return Command(
        update={"messages": [new_message]},
        goto="executor",
    )
