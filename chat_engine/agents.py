#!/usr/bin/env python
"""
Multi-Agent Workflow with Snowflake Cortex Integration

This module builds a data agent that can perform:
- Web research using Tavily Search
- Chart generation using Python REPL
- Snowflake data retrieval using Cortex Analyst (Text2SQL) and Cortex Search
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional, Type

from dotenv import load_dotenv
from langchain.schema import HumanMessage
from langchain_openai import ChatOpenAI
from langchain_tavily import TavilySearch
from langgraph.graph import END, START, StateGraph, MessagesState
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command
from pydantic import BaseModel
from snowflake.core import Root
from snowflake.core.cortex.lite_agent_service import AgentRunRequest
from snowflake.snowpark import Session
from scripts.utils.database_client import SnowflakeClient
from langfuse import observe

from tools import python_repl_tool
from prompts import executor_prompt, plan_prompt, agent_system_prompt

# Load environment variables
load_dotenv(override=True)


# Constants
MAX_REPLANS = 3

# Snowflake
SEMANTIC_MODEL_FILE = f"@{os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.{os.getenv('SNOWFLAKE_SEMANTIC_MODEL_STAGE', 'models')}/mint_semantic_model_2.yaml"


# State Definition
class State(MessagesState):
    """
    Custom state class with specific keys for the multi-agent workflow.

    Inherits from MessagesState which provides a 'messages' field that keeps
    track of the list of messages shared among agents.
    """

    user_query: Optional[str]  # The user's original query
    enabled_agents: Optional[
        List[str]
    ]  # Makes the system modular on which agents to include
    plan: Optional[
        List[Dict[int, Dict[str, Any]]]
    ]  # Steps in the plan to achieve the goal
    current_step: int  # Current step in the plan
    agent_query: Optional[
        str
    ]  # Tells the next agent exactly what to do at the current step
    last_reason: Optional[str]  # Explains the executor's decision for traceability
    replan_flag: Optional[bool]  # Indicates that the planner should revise the plan
    replan_attempts: Optional[
        Dict[int, Dict[int, int]]
    ]  # Replan attempts per step number


# Initialize LLMs
reasoning_llm = ChatOpenAI(
    model="o3",
    model_kwargs={"response_format": {"type": "json_object"}},
)

llm = ChatOpenAI(model="gpt-4o")


# Initialize tools
tavily_tool = TavilySearch(max_results=5)


# Snowflake Cortex Agent Implementation


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


# Node Definitions


@observe(name="planner_node")
def planner_node(state: State) -> Command[Literal["executor"]]:
    """
    Runs the planning LLM and stores the resulting plan in state.

    Takes the user's query and generates a plan consisting of numbered steps.
    Each step includes the action and the sub-agent assigned to that action.
    """
    llm_reply = reasoning_llm.invoke([plan_prompt(state)])

    try:
        content_str = (
            llm_reply.content
            if isinstance(llm_reply.content, str)
            else str(llm_reply.content)
        )
        parsed_plan = json.loads(content_str)
    except json.JSONDecodeError:
        raise ValueError(f"Planner returned invalid JSON:\n{llm_reply.content}")

    replan = state.get("replan_flag", False)
    updated_plan: Dict[str, Any] = parsed_plan

    return Command(
        update={
            "plan": updated_plan,
            "messages": [
                HumanMessage(
                    content=llm_reply.content,
                    name="replan" if replan else "initial_plan",
                )
            ],
            "user_query": state.get("user_query", state["messages"][0].content),
            "current_step": 1 if not replan else state["current_step"],
            "replan_flag": state.get("replan_flag", False),
            "last_reason": "",
            "enabled_agents": state.get("enabled_agents"),
        },
        goto="executor",
    )


@observe(name="executor_node")
def executor_node(
    state: State,
) -> Command[
    Literal[
        "web_researcher",
        "cortex_researcher",
        "chart_generator",
        "synthesizer",
        "planner",
    ]
]:
    """
    Executes the plan by identifying the sub-agent to run next.

    Generates instructions for the chosen agent and may decide to replan
    based on the results. If replanning is needed, it goes back to the planner.
    """
    plan: Dict[str, Any] = state.get("plan", {})
    step: int = state.get("current_step", 1)

    # If we just replanned, run the planned agent once before reconsidering
    if state.get("replan_flag"):
        planned_agent = plan.get(str(step), {}).get("agent")
        return Command(
            update={
                "replan_flag": False,
                "current_step": step + 1,
            },
            goto=planned_agent,
        )

    # Build prompt and call LLM
    llm_reply = reasoning_llm.invoke([executor_prompt(state)])
    try:
        content_str = (
            llm_reply.content
            if isinstance(llm_reply.content, str)
            else str(llm_reply.content)
        )
        parsed = json.loads(content_str)
        replan: bool = parsed["replan"]
        goto: str = parsed["goto"]
        reason: str = parsed["reason"]
        query: str = parsed["query"]
    except Exception as exc:
        raise ValueError(f"Invalid executor JSON:\n{llm_reply.content}") from exc

    updates: Dict[str, Any] = {
        "messages": [HumanMessage(content=llm_reply.content, name="executor")],
        "last_reason": reason,
        "agent_query": query,
    }

    replans: Dict[int, int] = state.get("replan_attempts", {}) or {}
    step_replans = replans.get(step, 0)

    # Handle replan decision
    if replan:
        if step_replans < MAX_REPLANS:
            replans[step] = step_replans + 1
            updates.update(
                {
                    "replan_attempts": replans,
                    "replan_flag": True,
                    "current_step": step,
                }
            )
            return Command(update=updates, goto="planner")
        else:
            next_agent = plan.get(str(step + 1), {}).get("agent", "synthesizer")
            updates["current_step"] = step + 1
            return Command(update=updates, goto=next_agent)

    # Happy path: run chosen agent
    planned_agent = plan.get(str(step), {}).get("agent")
    updates["current_step"] = step + 1 if goto == planned_agent else step
    updates["replan_flag"] = False
    return Command(update=updates, goto=goto)


# Agent Definitions

web_search_agent = create_react_agent(
    llm,
    tools=[tavily_tool],
    prompt=agent_system_prompt(
        """
        You are the Researcher. You can ONLY perform research
        by using the provided search tool (tavily_tool).
        When you have found the necessary information, end your output.
        Do NOT attempt to take further actions.
    """
    ),
)


@observe(name="web_research_node")
def web_research_node(state: State) -> Command[Literal["executor"]]:
    """
    Web research sub-agent node.

    A ReAct agent that uses Tavily Search tool to search the web and
    answer the sub-query assigned to it.
    """
    agent_query = state.get("agent_query")
    result = web_search_agent.invoke({"messages": agent_query})
    # Wrap in a human message, as not all providers allow
    # AI message at the last position of the input messages list
    result["messages"][-1] = HumanMessage(
        content=result["messages"][-1].content, name="web_researcher"
    )
    return Command(
        update={"messages": result["messages"]},
        goto="executor",
    )


# NOTE: THIS PERFORMS ARBITRARY CODE EXECUTION,
# WHICH CAN BE UNSAFE WHEN NOT SANDBOXED
chart_agent = create_react_agent(
    llm,
    [python_repl_tool],
    prompt=agent_system_prompt(
        """
        You can only generate charts. You are working with a researcher
        colleague.
        1) Print the chart first.
        2) Save the chart to a file in the current working directory.
        3) At the very end of your message, output EXACTLY two lines
        so the summarizer can find them:
           CHART_PATH: <relative_path_to_chart_file>
           CHART_NOTES: <one concise sentence summarizing the main insight in the chart>
        Do not include any other trailing text after these two lines.
        """
    ),
)


@observe(name="chart_node")
def chart_node(state: State) -> Command[Literal["chart_summarizer"]]:
    """
    Chart generation sub-agent node.

    Generates Python code that creates a chart and executes it using python_repl_tool.
    """
    result = chart_agent.invoke(state)
    # Wrap in a human message
    result["messages"][-1] = HumanMessage(
        content=result["messages"][-1].content, name="chart_generator"
    )
    return Command(
        update={"messages": result["messages"]},
        goto="chart_summarizer",
    )


chart_summary_agent = create_react_agent(
    llm,
    tools=[],
    prompt=agent_system_prompt(
        "You can only generate image captions. You are working with a researcher colleague "
        "and a chart generator colleague. Your task is to generate a standalone, concise "
        "summary for the provided chart image saved at a local PATH, where the PATH should "
        "be and only be provided by your chart generator colleague. The summary should be "
        "no more than 3 sentences and should not mention the chart itself."
    ),
)


@observe(name="chart_summary_node")
def chart_summary_node(state: State) -> Command[Literal[END]]:
    """
    Chart summary sub-agent node.

    Generates a caption describing the chart generated by the chart generator.
    """
    result = chart_summary_agent.invoke(state)
    print(f"Chart summarizer answer: {result['messages'][-1].content}")
    return Command(
        update={
            "messages": result["messages"],
            "final_answer": result["messages"][-1].content,
        },
        goto=END,
    )


@observe(name="cortex_research_node")
def cortex_research_node(state: State) -> Command[Literal["executor"]]:
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
                "messages": [
                    HumanMessage(content=error_message, name="cortex_researcher")
                ]
            },
            goto="executor",
        )

    query = state.get("agent_query", state.get("user_query", ""))
    agent_response = cortex_agent.invoke({"messages": query})
    new_message = HumanMessage(
        content=agent_response["messages"][-1].content, name="cortex_researcher"
    )
    return Command(
        update={"messages": [new_message]},
        goto="executor",
    )


@observe(name="synthesizer_node")
def synthesizer_node(state: State) -> Command[Literal[END]]:
    """
    Synthesizer (text summarizer) sub-agent node.

    Creates a concise, human-readable summary of the entire interaction
    purely in prose. Generates text that summarizes the retrieved results
    when the user does not ask for charts.
    """
    relevant_msgs = [
        m.content
        for m in state.get("messages", [])
        if getattr(m, "name", None)
        in (
            "web_researcher",
            "cortex_researcher",
            "chart_generator",
            "chart_summarizer",
        )
    ]

    user_question = state.get(
        "user_query",
        state.get("messages", [{}])[0].content if state.get("messages") else "",
    )

    synthesis_instructions = """
        You are the Synthesizer. Use the context below to directly
        answer the user's question. Perform any lightweight calculations,
        comparisons, or inferences required. Do not invent facts not
        supported by the context. If data is missing, say what's missing
        and, if helpful, offer a clearly labeled best-effort estimate
        with assumptions.

        Produce a concise response that fully answers the question, with
        the following guidance:
        - Start with the direct answer (one short paragraph or a tight bullet list).
        - Include key figures from any 'Results:' tables (e.g., totals, top items).
        - If any message contains citations, include them as a brief 'Citations: [...]' line.
        - Keep the output crisp; avoid meta commentary or tool instructions.
    """

    summary_prompt = [
        HumanMessage(
            content=(
                f"User question: {user_question}\n\n"
                f"{synthesis_instructions}\n\n"
                f"Context:\n\n" + "\n\n---\n\n".join(relevant_msgs)
            )
        )
    ]

    llm_reply = llm.invoke(summary_prompt)
    answer = llm_reply.content.strip()
    print(f"Synthesizer answer: {answer}")

    return Command(
        update={
            "final_answer": answer,
            "messages": [HumanMessage(content=answer, name="synthesizer")],
        },
        goto=END,
    )
