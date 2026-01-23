#!/usr/bin/env python
"""
Synthesizer Agent - Generates final text summaries.
"""

from typing import Literal
from langchain_core.messages import HumanMessage
from langgraph.graph import END
from langgraph.types import Command
from langfuse import observe

from .shared import State, llm


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
            "researcher",
            "cortex_analyst",
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
