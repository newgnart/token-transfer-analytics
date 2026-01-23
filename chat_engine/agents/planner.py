#!/usr/bin/env python
"""
Planner Agent - Generates and revises execution plans.
"""

import json
from typing import Literal, Any, Dict
from langchain_core.messages import HumanMessage
from langgraph.types import Command
from langfuse import observe

from .shared import State, reasoning_llm
from chat_engine.prompts import plan_prompt


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
