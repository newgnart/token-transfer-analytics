import hypersync
import asyncio
from dotenv import load_dotenv
import os
import logging
import argparse
import polars as pl
from pathlib import Path
import re
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
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
        logger.warning(
            f"No valid block numbers found in filenames for prefix: {prefix}"
        )

    return max_block


def get_client():
    """Initialize HyperSync client with bearer token authentication."""
    bearer_token = os.getenv("HYPERSYNC_BEARER_TOKEN")
    if not bearer_token:
        raise ValueError("HYPERSYNC_BEARER_TOKEN environment variable is required")

    return hypersync.HypersyncClient(
        hypersync.ClientConfig(
            url="https://eth.hypersync.xyz",
            bearer_token=bearer_token,
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

    await client.collect_parquet(
        query=query, path=".data", config=hypersync.StreamConfig()
    )

    logger.info("Collection complete!")


async def collect_logs_incremental(
    contract: str,
    prefix: str,
    data_dir: str = ".data",
    to_block: int | None = None,
    from_block: int | None = None,
) -> tuple[int, int, str]:
    """
    Collect logs incrementally from the latest fetched block to the current block.
    Designed for daily/scheduled execution.

    Args:
        contract: Contract address to query
        prefix: Filename prefix (e.g., 'open_logs')
        data_dir: Directory for data files (default: '.data')
        to_block: Ending block (None for latest chain block)
        from_block: Starting block (None to auto-detect from existing files)

    Returns:
        Tuple of (from_block, to_block, output_file_path)

    Raises:
        ValueError: If no previous data exists and from_block is not provided
    """
    client = get_client()

    # Auto-detect starting block if not provided
    if from_block is None:
        latest_block = get_latest_fetched_block(prefix, data_dir)

        if latest_block is None:
            raise ValueError(
                f"No existing data found for prefix '{prefix}'. "
                "Please provide --from-block for initial data collection."
            )

        # Start from the next block after the latest fetched
        from_block = latest_block
        logger.info(f"Auto-detected starting block: {from_block}")

    logger.info(f"=== Incremental Log Collection ===")
    logger.info(f"Contract: {contract}")
    logger.info(f"Prefix: {prefix}")
    logger.info(f"From block: {from_block}")
    logger.info(f"To block: {to_block if to_block else 'latest'}")

    # Collect data to temporary file
    temp_file = Path(data_dir) / "logs.parquet"
    await collect_logs(contract=contract, from_block=from_block, to_block=to_block)

    # Check if file was created (HyperSync doesn't create file if no data)
    if not temp_file.exists():
        logger.warning(
            f"No new data found in block range {from_block} - {to_block if to_block else 'latest'}"
        )
        logger.info("✓ Incremental collection complete (no new data)")
        return from_block, from_block, None

    # Read actual block range from the collected data
    try:
        df = pl.scan_parquet(temp_file)
        actual_from_block = df.select(pl.col("block_number").min()).collect().item()
        actual_to_block = df.select(pl.col("block_number").max()).collect().item()
        record_count = df.select(pl.len()).collect().item()

        logger.info(f"Collected {record_count} records")
        logger.info(f"Actual block range: {actual_from_block} - {actual_to_block}")

        # Rename with actual block range
        output_file = (
            Path(data_dir) / f"{prefix}_{actual_from_block}_{actual_to_block}.parquet"
        )
        temp_file.rename(output_file)

        logger.info(f"✓ Saved to: {output_file}")

        return actual_from_block, actual_to_block, str(output_file)

    except Exception as e:
        logger.error(f"Failed to process collected data: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Incrementally collect blockchain logs for daily/scheduled execution"
    )
    parser.add_argument(
        "--contract",
        "-c",
        required=True,
        help="Contract address to query (e.g., 0x...)",
    )
    parser.add_argument(
        "--prefix",
        "-p",
        required=True,
        help="Filename prefix for parquet files (e.g., 'open_logs')",
    )
    parser.add_argument(
        "--data-dir",
        "-d",
        default=".data",
        help="Directory for data files (default: '.data')",
    )
    parser.add_argument(
        "--from-block",
        "-f",
        type=int,
        default=None,
        help="Starting block (default: auto-detect from existing files)",
    )
    parser.add_argument(
        "--to-block",
        "-t",
        type=int,
        default=None,
        help="Ending block (default: latest chain block)",
    )

    args = parser.parse_args()

    try:
        from_block, to_block, output_file = asyncio.run(
            collect_logs_incremental(
                contract=args.contract,
                prefix=args.prefix,
                data_dir=args.data_dir,
                from_block=args.from_block,
                to_block=args.to_block,
            )
        )

        logger.info("=== Collection Summary ===")
        logger.info(f"Block range: {from_block} → {to_block}")
        if output_file:
            logger.info(f"Output file: {output_file}")
            logger.info("✓ Incremental collection complete!")
        else:
            logger.info("No new data collected")

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        exit(1)
    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
