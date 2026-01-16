"""
Airflow DAG for loading parquet files to Snowflake.

This DAG loads the most recently collected parquet file to Snowflake.
It is triggered by the orchestrate_load_and_dbt master DAG.
"""

import re
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv

load_dotenv()


# ============================================================================
# Utility Functions
# ============================================================================


def get_latest_parquet_file(
    prefix: str, data_dir: str = "/opt/airflow/.data"
) -> str | None:
    """
    Get the path to the most recently created parquet file matching a prefix.

    Args:
        prefix: Prefix to search for (e.g., 'open_logs')
        data_dir: Directory containing parquet files

    Returns:
        Path to the latest parquet file, or None if no matching files exist
    """
    import logging

    logger = logging.getLogger(__name__)

    data_path = Path(data_dir)
    if not data_path.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return None

    # Find all parquet files matching the prefix
    pattern = f"{prefix}_*.parquet"
    matching_files = list(data_path.glob(pattern))

    if not matching_files:
        logger.warning(f"No parquet files found with prefix: {prefix}")
        return None

    # Expected format: prefix_YYMMDD_fromBlock_toBlock.parquet
    # Sort by date (YYMMDD) and then by to_block to get the most recent
    block_pattern = re.compile(rf"{re.escape(prefix)}_(\d{{6}})_(\d+)_(\d+)\.parquet")

    valid_files = []
    for file in matching_files:
        match = block_pattern.match(file.name)
        if match:
            date_str = match.group(1)
            to_block = int(match.group(3))
            valid_files.append((file, date_str, to_block))

    if not valid_files:
        logger.warning(f"No valid parquet files found for prefix: {prefix}")
        return None

    # Sort by date descending, then by to_block descending
    valid_files.sort(key=lambda x: (x[1], x[2]), reverse=True)
    latest_file = valid_files[0][0]

    logger.info(f"Latest parquet file: {latest_file}")
    return str(latest_file)


# ============================================================================
# DAG: Load Logs to Snowflake
# ============================================================================


@dag(
    dag_id="load_logs_sf",
    schedule=None,  # Triggered by orchestrate_load_and_dbt DAG
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["snowflake", "logs", "load"],
    max_active_runs=1,
    description="Load collected parquet files to Snowflake",
    params={
        "prefix": "open_logs",
        "snowflake_schema": "raw",
        "snowflake_table": "open_logs",
        "snowflake_write_disposition": "append",
        "file_path": None,  # Optional: explicit file path to load
    },
)
def load_logs_sf():
    """
    Load parquet files to Snowflake.

    This DAG:
    1. Finds the latest parquet file matching the prefix (or uses explicit file_path)
    2. Loads the file to the specified Snowflake table

    Parameters (configurable via UI or CLI):
    - prefix: Filename prefix to search for (default: 'open_logs')
    - snowflake_schema: Target Snowflake schema (default: 'raw')
    - snowflake_table: Target Snowflake table (default: 'open_logs')
    - snowflake_write_disposition: Write mode ('append', 'replace', 'merge')
    - file_path: Optional explicit file path to load (overrides prefix search)
    """

    @task
    def find_file_to_load(**context) -> str | None:
        """
        Find the parquet file to load.

        Returns:
            Path to the parquet file, or None if no file found
        """
        params = context["params"]
        explicit_file_path = params.get("file_path")
        prefix = params.get("prefix", "open_logs")
        data_dir = "/opt/airflow/.data"

        # Use explicit file path if provided
        if explicit_file_path:
            if Path(explicit_file_path).exists():
                print(f"Using explicit file path: {explicit_file_path}")
                return explicit_file_path
            else:
                raise FileNotFoundError(
                    f"Specified file not found: {explicit_file_path}"
                )

        # Otherwise, find the latest file matching the prefix
        file_path = get_latest_parquet_file(prefix, data_dir)

        if file_path:
            print(f"Found latest parquet file: {file_path}")
            return file_path
        else:
            print(f"No parquet files found with prefix: {prefix}")
            return None

    @task.bash
    def load_to_snowflake(file_path: str | None, **context) -> str:
        """
        Load parquet file to Snowflake table using load_file.py script.

        Args:
            file_path: Path to the parquet file to load

        Returns:
            Bash command to execute
        """
        params = context["params"]
        snowflake_schema = params.get("snowflake_schema", "raw")
        snowflake_table = params.get("snowflake_table", "open_logs")
        snowflake_write_disposition = params.get(
            "snowflake_write_disposition", "append"
        )

        if not file_path:
            print("No file to load, skipping Snowflake load")
            return "echo 'No file to load'"

        return f"""
        cd /opt/airflow && \
        python scripts/load_file.py \
            -f {file_path} \
            -c snowflake \
            -s {snowflake_schema} \
            -t {snowflake_table} \
            -w {snowflake_write_disposition} \
        """

    # Define task dependencies
    file_path = find_file_to_load()
    load_to_snowflake(file_path)


# Create DAG instance
load_logs_sf()
