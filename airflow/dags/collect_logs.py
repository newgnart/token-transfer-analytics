"""
Airflow DAG for incremental log collection from HyperSync.

This DAG collects blockchain logs incrementally and saves them as parquet files.
It is triggered by the orchestrate_load_and_dbt master DAG.
"""

import os
import re
import asyncio
from datetime import datetime
from pathlib import Path

import hypersync
import polars as pl
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv

load_dotenv()


# ============================================================================
# Utility Functions
# ============================================================================


def get_latest_fetched_block(
    prefix: str, data_dir: str = "/opt/airflow/.data"
) -> int | None:
    """
    Get the latest (maximum) block number from parquet files matching a prefix.

    Args:
        prefix: Prefix to search for (e.g., 'open_logs')
        data_dir: Directory containing parquet files (default: '/opt/airflow/.data')

    Returns:
        Maximum block number found, or None if no matching files exist
    """
    import logging

    logger = logging.getLogger(__name__)

    data_path = Path(data_dir)
    logger.info(f"Checking for existing files in: {data_dir}")
    logger.info(f"Directory exists: {data_path.exists()}")

    if not data_path.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return None

    # Find all parquet files matching the prefix
    pattern = f"{prefix}_*.parquet"
    matching_files = list(data_path.glob(pattern))
    logger.info(f"Found {len(matching_files)} files matching pattern '{pattern}'")

    if not matching_files:
        logger.warning(f"No parquet files found with prefix: {prefix}")
        # List all files in directory for debugging
        all_files = list(data_path.glob("*.parquet"))
        logger.info(f"All parquet files in directory: {[f.name for f in all_files]}")
        return None

    # Extract block numbers from filenames
    # Expected format: prefix_YYMMDD_fromBlock_toBlock.parquet (e.g., open_logs_260101_12345_67890.parquet)
    max_block = None
    block_pattern = re.compile(rf"{re.escape(prefix)}_\d{{6}}_(\d+)_(\d+)\.parquet")

    for file in matching_files:
        match = block_pattern.match(file.name)
        if match:
            from_block = int(match.group(1))
            to_block = int(match.group(2))

            # Track the maximum to_block
            if max_block is None or to_block > max_block:
                max_block = to_block

            logger.info(f"Found file: {file.name} (blocks {from_block}-{to_block})")
        else:
            logger.warning(f"File does not match expected pattern: {file.name}")

    if max_block:
        logger.info(f"Latest fetched block for '{prefix}': {max_block}")
    else:
        logger.warning(
            f"No valid block numbers found in filenames for prefix: {prefix}"
        )

    return max_block


def get_hypersync_client():
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


async def collect_logs_async(
    contract: str, from_block: int, to_block: int | None, data_dir: str
):
    """
    Collect historical logs data for a contract and save to Parquet.

    Args:
        contract: Contract address to query
        from_block: Starting block number
        to_block: Ending block number (None for latest)
        data_dir: Directory to save parquet files
    """
    client = get_hypersync_client()

    # Build query
    query = hypersync.preset_query_logs(
        contract, from_block=from_block, to_block=to_block
    )

    print(f"Collecting logs for contract: {contract}")
    print(f"From block: {from_block}")
    print(f"To block: {to_block if to_block else 'latest'}")

    await client.collect_parquet(
        query=query, path=data_dir, config=hypersync.StreamConfig()
    )

    print("Collection complete!")


# ============================================================================
# DAG: Collect Logs from HyperSync
# ============================================================================


@dag(
    dag_id="collect_logs",
    schedule=None,  # Triggered by orchestrate_load_and_dbt DAG
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["hypersync", "logs", "incremental", "extract"],
    max_active_runs=1,
    description="Incrementally collect blockchain logs from HyperSync",
    params={
        "from_block": None,
        "to_block": None,
        "contract": "0x323c03c48660fE31186fa82c289b0766d331Ce21",
        "prefix": "open_logs",
    },
)
def collect_logs():
    """
    Collect logs from HyperSync and save as parquet files.

    This DAG:
    1. Auto-detects the latest fetched block from existing files
    2. Collects new logs from that block to the current block
    3. Saves the result with today's date and block range in the filename

    Parameters (configurable via UI or CLI):
    - from_block: Starting block number (None = auto-detect from existing files)
    - to_block: Ending block number (None = latest chain block)
    - contract: Contract address to query
    - prefix: Filename prefix for saved parquet files
    """

    @task
    def collect_logs_incremental(**context) -> dict:
        """
        Collect logs incrementally from the latest fetched block to the current block.

        Returns:
            Dictionary with collection metadata including output_file path
        """
        # Get parameters from DAG run config
        params = context["params"]
        contract = params.get("contract", "0x323c03c48660fE31186fa82c289b0766d331Ce21")
        prefix = params.get("prefix", "open_logs")
        from_block = params.get("from_block")
        to_block = params.get("to_block")
        data_dir = "/opt/airflow/.data"

        # Ensure data directory exists
        Path(data_dir).mkdir(parents=True, exist_ok=True)

        # Auto-detect starting block if not provided
        if from_block is None:
            latest_block = get_latest_fetched_block(prefix, data_dir)

            if latest_block is None:
                raise ValueError(
                    f"No existing data found for prefix '{prefix}'. "
                    "Please provide from_block for initial data collection."
                )

            # Start from the next block after the latest fetched
            from_block = latest_block + 1
            print(f"Auto-detected starting block: {from_block}")

        print(f"=== Incremental Log Collection ===")
        print(f"Contract: {contract}")
        print(f"Prefix: {prefix}")
        print(f"From block: {from_block}")
        print(f"To block: {to_block if to_block else 'latest'}")

        # Collect data to temporary file
        temp_file = Path(data_dir) / "logs.parquet"
        asyncio.run(
            collect_logs_async(
                contract=contract,
                from_block=from_block,
                to_block=to_block,
                data_dir=data_dir,
            )
        )

        # Check if file was created (HyperSync doesn't create file if no data)
        if not temp_file.exists():
            print(
                f"No new data found in block range {from_block} - {to_block if to_block else 'latest'}"
            )
            print("✓ Incremental collection complete (no new data)")
            return {
                "status": "no_data",
                "from_block": from_block,
                "to_block": from_block,
                "output_file": None,
            }

        # Read actual block range from the collected data
        try:
            df = pl.scan_parquet(temp_file)
            actual_from_block = df.select(pl.col("block_number").min()).collect().item()
            actual_to_block = df.select(pl.col("block_number").max()).collect().item()
            record_count = df.select(pl.len()).collect().item()

            print(f"Collected {record_count} records")
            print(f"Actual block range: {actual_from_block} - {actual_to_block}")

            # Rename with today's date and actual block range
            today = datetime.now().strftime("%y%m%d")
            output_file = (
                Path(data_dir)
                / f"{prefix}_{today}_{actual_from_block}_{actual_to_block}.parquet"
            )
            temp_file.rename(output_file)

            print(f"✓ Saved to: {output_file}")

            return {
                "status": "success",
                "from_block": actual_from_block,
                "to_block": actual_to_block,
                "output_file": str(output_file),
                "record_count": record_count,
            }

        except Exception as e:
            print(f"Failed to process collected data: {e}")
            raise

    # Execute task
    collect_logs_incremental()


# Create DAG instance
collect_logs()
