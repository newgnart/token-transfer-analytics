import hypersync
import asyncio
from dotenv import load_dotenv
import os, logging
import argparse
import polars as pl

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_client():
    return hypersync.HypersyncClient(
        hypersync.ClientConfig(
            url="https://eth.hypersync.xyz",
            bearer_token=os.getenv("HYPERSYNC_BEARER_TOKEN"),
        )
    )


async def collect_logs(contract: str, from_block: int, to_block: int | None = None):
    """
    Collect historical logs data for a contract and save to Parquet.

    Args:
        contract: Contract address to query
        from_block: Starting block number
        to_block: Ending block number (None for latest)
    """
    client = get_client()

    # Build query
    query = hypersync.preset_query_logs(
        contract, from_block=from_block, to_block=to_block
    )

    logger.info(f"Collecting logs for contract: {contract}")
    logger.info(f"From block: {from_block}")
    logger.info(f"To block: {to_block if to_block else 'latest'}")

    # Run the query - automatically paginated
    # The query returns when it reaches some limit (time, response size etc.)
    # There is a next_block field on the response so we can continue until
    # res.next_block equals res.archive_height or query.to_block
    await client.collect_parquet(
        query=query, path=".data", config=hypersync.StreamConfig()
    )

    logger.info("Collection complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Collect historical blockchain logs data using HyperSync"
    )
    parser.add_argument(
        "--contract",
        "-c",
        required=True,
        help="Contract address to query (e.g., 0x...)",
    )
    parser.add_argument(
        "--from-block", "-f", type=int, required=True, help="Starting block number"
    )
    parser.add_argument(
        "--to-block",
        "-t",
        type=int,
        default=None,
        help="Ending block number (default: None for latest)",
    )
    parser.add_argument(
        "--contract-name",
        "-n",
        type=str,
        default=None,
        help="Contract name to use in the filename",
    )

    args = parser.parse_args()

    asyncio.run(
        collect_logs(
            contract=args.contract, from_block=args.from_block, to_block=args.to_block
        )
    )

    # Rename parquet file with block range suffix
    source_path = f".data/logs.parquet"
    # if args.contract_name:
    #     source_path = f".data/{args.contract_name}.parquet"

    try:
        # Read the actual to_block from the parquet file
        to_block_actual = (
            pl.scan_parquet(source_path)
            .select(pl.col("block_number").max())
            .collect()
            .item()
        )

        # Construct new filename with block range
        dest_path = (
            f".data/{args.contract_name}_{args.from_block}_{to_block_actual}.parquet"
        )
        if args.contract_name:
            dest_path = f".data/{args.contract_name}_logs_{args.from_block}_{to_block_actual}.parquet"

        os.rename(source_path, dest_path)
        logger.info(f"Renamed file to: {dest_path}")

    except FileNotFoundError:
        logger.error(f"Parquet file not found: {source_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to rename parquet file: {e}")
        raise


if __name__ == "__main__":
    main()
