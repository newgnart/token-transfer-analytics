#!/usr/bin/env python
"""
Web Research Agent - Searches the web using Tavily.
"""

from typing import Literal
from langchain_core.messages import HumanMessage
from langchain_tavily import TavilySearch
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command
from langfuse import observe

from .shared import State, llm
from ..prompts import agent_system_prompt
from ..tools import retrieve_knowledge_tool


# Initialize Tavily search tool
tavily_tool = TavilySearch(max_results=5)


# Create web search agent
researcher_agent = create_react_agent(
    llm,
    tools=[tavily_tool, retrieve_knowledge_tool],
    prompt=agent_system_prompt(
        """
        You are the Researcher. You can  perform research
        by using the provided search tools (tavily_tool and retrieve_knowledge_tool).
        The retrieve_knowledge_tool is used to search the knowledge base for relevant information about Open Stablecoin Index.
        The tavily_tool is used to search the web for relevant information.
        When you have found the necessary information, end your output.
        Do NOT attempt to take further actions.
    """
    ),
)


@observe(name="researcher_node")
def researcher_node(state: State) -> Command[Literal["executor"]]:
    """
    Researcher sub-agent node.

    A ReAct agent that uses tavily_tool and retrieve_knowledge_tool to search the web and knowledge base and
    answer the sub-query assigned to it.
    """
    agent_query = state.get("agent_query")
    result = researcher_agent.invoke({"messages": agent_query})
    # Wrap in a human message, as not all providers allow
    # AI message at the last position of the input messages list
    result["messages"][-1] = HumanMessage(
        content=result["messages"][-1].content, name="researcher"
    )
    return Command(
        update={"messages": result["messages"]},
        goto="executor",
    )
