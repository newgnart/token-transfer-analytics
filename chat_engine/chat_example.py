from chat_engine.chat_engine import chat_engine


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
