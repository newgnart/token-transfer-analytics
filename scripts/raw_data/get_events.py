from onchaindata.data_extraction.etherscan import EtherscanClient
import os
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv
from eth_utils import event_signature_to_log_topic
import polars as pl

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_events(chainid: int, contract: str, name: str = None):
    """
    Extract event signatures and topic0 hashes from a contract's ABI.

    Args:
        chainid: Chain ID (e.g., 1 for Ethereum mainnet)
        contract: Contract address to query (e.g., 0x...)
        name: Optional filename for saving the DataFrame. If not provided, uses contract address.
    """
    logger.info(f"Fetching events for contract: {contract}")
    logger.info(f"Chain ID: {chainid}")

    # Initialize Etherscan client
    etherscan_client = EtherscanClient(
        chainid=chainid, api_key=os.getenv("ETHERSCAN_API_KEY")
    )

    # Get contract ABI (including implementation ABI for proxy contracts)
    abi, implementation_abi = etherscan_client.get_contract_abi(
        contract, save_dir=".data/abi"
    )

    # Extract events from both ABIs
    events_from_abi = [item for item in abi if item["type"] == "event"]
    if implementation_abi:
        events_from_abi += [
            item for item in implementation_abi if item["type"] == "event"
        ]

    logger.info(f"Found {len(events_from_abi)} events in contract ABI")

    # Build event data with topic0 hashes
    event_data = []
    for event in events_from_abi:
        # Build the event signature string
        param_types = [param["type"] for param in event["inputs"]]
        signature = f"{event['name']}({','.join(param_types)})"

        # Get the topic0 hash
        topic0 = "0x" + event_signature_to_log_topic(signature).hex()

        event_data.append(
            {
                "topic0": topic0,
                "event_name": event["name"],
                "signature": signature,
                "inputs": event["inputs"],
            }
        )

    # Create DataFrame
    event_signature_df = pl.DataFrame(event_data)

    logger.info("Event extraction complete!")

    # Save DataFrame to .data folder
    data_dir = Path(".data")
    data_dir.mkdir(exist_ok=True)

    # Use provided name or fallback to contract address
    filename = name if name else contract
    output_path = data_dir / f"{filename}_events.json"

    event_signature_df.write_json(output_path)
    logger.info(f"Saved events to: {output_path}")

    output_path = data_dir / f"{filename}_events.csv"
    event_signature_df.select("topic0", "event_name", "signature").write_csv(
        output_path
    )

    return event_signature_df


def main():
    parser = argparse.ArgumentParser(
        description="Extract event signatures and topic0 hashes from contract ABI"
    )
    parser.add_argument(
        "--chainid",
        "-c",
        type=int,
        required=True,
        help="Chain ID (e.g., 1 for Ethereum mainnet, 137 for Polygon)",
    )
    parser.add_argument(
        "--contract",
        "-a",
        required=True,
        help="Contract address to query (e.g., 0x...)",
    )
    parser.add_argument(
        "--name",
        "-n",
        required=False,
        help="Optional filename for saving the DataFrame (without extension). If not provided, uses contract address.",
    )

    args = parser.parse_args()

    get_events(chainid=args.chainid, contract=args.contract, name=args.name)


if __name__ == "__main__":
    main()
