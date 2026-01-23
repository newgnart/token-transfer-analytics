#!/usr/bin/env python
"""
Executor Agent - Orchestrates execution of the plan.
"""

import json
from typing import Literal, Any, Dict
from langchain_core.messages import HumanMessage
from langgraph.types import Command
from langfuse import observe

from .shared import State, reasoning_llm, MAX_REPLANS
from ..prompts import executor_prompt


@observe(name="executor_node")
def executor_node(
    state: State,
) -> Command[
    Literal[
        "researcher",
        "cortex_analyst",
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
