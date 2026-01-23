from typing import Optional, List
from dotenv import load_dotenv

load_dotenv(override=True)
from langchain_core.messages import HumanMessage
from langfuse import observe

from langgraph.graph import StateGraph, START
from .agents import (
    State,
    planner_node,
    executor_node,
    researcher_node,
    chart_node,
    chart_summary_node,
    synthesizer_node,
    cortex_analyst_node,
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
    workflow.add_node("researcher", researcher_node)
    workflow.add_node("chart_generator", chart_node)
    workflow.add_node("chart_summarizer", chart_summary_node)
    workflow.add_node("synthesizer", synthesizer_node)
    workflow.add_node("cortex_analyst", cortex_analyst_node)

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
    if enabled_agents is None:
        default_agents = [
            "researcher",
            "chart_generator",
            "chart_summarizer",
            "synthesizer",
            "cortex_analyst",
        ]
        enabled_agents = default_agents

    state = {
        "messages": [HumanMessage(content=query)],
        "user_query": query,
        "enabled_agents": enabled_agents,
    }

    graph = build_graph()
    return graph.invoke(state)


def visualize_graph(output_path: str = "graph.png"):
    """
    Save the graph structure as a PNG file.

    Args:
        output_path: Path where the PNG file will be saved. Defaults to "graph.png"
    """
    graph = build_graph()
    png_data = graph.get_graph().draw_png()

    with open(output_path, "wb") as f:
        f.write(png_data)

    print(f"Graph saved to {output_path}")


def main():
    """Main execution function with example queries."""
    # query1 = "What is open stablecoin index?"
    # print(f"Query: {query1}")
    # chat_engine(query1)
    # print("--------------------------------\n")

    # query2 = "What are the top 3 minting addresses in fct_mint table?"
    # print(f"Query: {query2}")
    # chat_engine(query2)
    # print("--------------------------------\n")

    query3 = "Give me the chart of the top 3 minting addresses in the fct_mint table?"
    # without saying fct_mint tracing: https://us.cloud.langfuse.com/project/cmkf6fdvf000rad07vt5hp9k0/traces/25a46aa42bf8dc0ff218ecdbc7eb4dcf?observation=8bcf14c9052b0daa&timestamp=2026-01-18T05:49:57.382Z
    # final answer: I have been instructed to generate a chart using given data. Could you provide the minting data?

    # with explicitly mentioning
    # https://us.cloud.langfuse.com/project/cmkf6fdvf000rad07vt5hp9k0/traces?peek=0e635a5bef8004f14f20062df7212c29&timestamp=2026-01-18T05%3A54%3A00.793Z&observation=f8a1b0d333e8d659
    print(f"Query: {query3}")
    chat_engine(query3)
    print("--------------------------------\n")


if __name__ == "__main__":
    main()
    # visualize_graph("chat_engine_graph.png")
