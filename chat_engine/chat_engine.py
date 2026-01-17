from typing import Optional, List
from dotenv import load_dotenv

load_dotenv(override=True)
from langchain.schema import HumanMessage
from langfuse import observe

from langgraph.graph import StateGraph, START
from agents import (
    State,
    planner_node,
    executor_node,
    web_research_node,
    chart_node,
    chart_summary_node,
    synthesizer_node,
    cortex_research_node,
)


# Build the graph
def build_graph():
    """
    Build and compile the multi-agent workflow graph.

    Args:
        include_cortex: Whether to include the Cortex researcher node.
                       Defaults to True if Cortex Agent is available.
    """
    workflow = StateGraph(State)
    workflow.add_node("planner", planner_node)
    workflow.add_node("executor", executor_node)
    workflow.add_node("web_researcher", web_research_node)
    workflow.add_node("chart_generator", chart_node)
    workflow.add_node("chart_summarizer", chart_summary_node)
    workflow.add_node("synthesizer", synthesizer_node)

    workflow.add_node("cortex_researcher", cortex_research_node)

    workflow.add_edge(START, "planner")

    return workflow.compile()


@observe(name="chat_engine")
def chat_engine(query: str, enabled_agents: Optional[List[str]] = None):
    """
    Run the multi-agent workflow with a given query.

    Args:
        query: The user's query string
        enabled_agents: List of enabled agent names. Defaults to all agents.

    Returns:
        The result of the graph invocation
    """
    # Update Langfuse context with metadata
    # langfuse_context.update_current_trace(
    #     user_id="user",
    #     session_id="chat_session",
    #     metadata={"query": query, "enabled_agents": enabled_agents},
    # )
    if enabled_agents is None:
        default_agents = [
            "web_researcher",
            "chart_generator",
            "chart_summarizer",
            "synthesizer",
            "cortex_researcher",
        ]
        enabled_agents = default_agents

    state = {
        "messages": [HumanMessage(content=query)],
        "user_query": query,
        "enabled_agents": enabled_agents,
    }

    graph = build_graph()
    return graph.invoke(state)


def main():
    """Main execution function with example queries."""
    # query1 = "What is openai's latest product?"
    # print(f"Query: {query1}")
    # chat_engine(query1)
    # print("--------------------------------\n")

    query2 = "What are the top 3 minting addresses in fct_mint table?"
    print(f"Query: {query2}")
    chat_engine(query2)
    print("--------------------------------\n")


if __name__ == "__main__":
    main()
