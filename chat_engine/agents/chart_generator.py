#!/usr/bin/env python
"""
Chart Generator Agent - Creates visualizations using Python.
"""

from typing import Literal
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command
from langfuse import observe

from .shared import State, llm
from ..prompts import agent_system_prompt
from ..tools import python_repl_tool


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
