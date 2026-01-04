import hypersync
import asyncio
from dotenv import load_dotenv
import os, logging
import argparse
import polars as pl
from pathlib import Path
import re

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_latest_fetched_block(prefix: str, data_dir: str = ".data") -> int | None:
    """
    Get the latest (maximum) block number from parquet files matching a prefix.

    Args:
        prefix: Prefix to search for (e.g., 'open_logs')
        data_dir: Directory containing parquet files (default: '.data')

    Returns:
        Maximum block number found, or None if no matching files exist

    Example:
        >>> get_latest_fetched_block('open_logs')
        24107494  # from open_logs_22269355_24107494.parquet
    """
    data_path = Path(data_dir)

    if not data_path.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return None

    # Find all parquet files matching the prefix
    pattern = f"{prefix}_*.parquet"
    matching_files = list(data_path.glob(pattern))

    if not matching_files:
        logger.info(f"No parquet files found with prefix: {prefix}")
        return None

    # Extract block numbers from filenames
    # Expected format: prefix_fromBlock_toBlock.parquet
    max_block = None
    block_pattern = re.compile(rf"{re.escape(prefix)}_(\d+)_(\d+)\.parquet")

    for file in matching_files:
        match = block_pattern.match(file.name)
        if match:
            from_block = int(match.group(1))
            to_block = int(match.group(2))

            # Track the maximum to_block
            if max_block is None or to_block > max_block:
                max_block = to_block

            logger.debug(f"Found file: {file.name} (blocks {from_block}-{to_block})")

    if max_block:
        logger.info(f"Latest fetched block for '{prefix}': {max_block}")
    else:
        logger.warning(f"No valid block numbers found in filenames for prefix: {prefix}")

    return max_block


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
