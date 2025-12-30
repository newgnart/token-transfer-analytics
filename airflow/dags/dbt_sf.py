"""
### Capstone dbt Orchestration DAG
A simple dbt orchestration DAG that matches the actual capstone implementation.
Uses @task.bash with dbt build command, following the capstone pattern.

This DAG demonstrates:
- Simple dbt integration using bash commands
- Environment variable configuration from Airflow connections
- The recommended 1 task = 1 dbt pipeline approach
"""

import base64
import json
import os

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import pendulum


def get_dbt_snowflake_env_vars():
    """
    Get dbt Snowflake environment variables from Airflow connections.
    Decodes base64-encoded private key using Python since Jinja2 doesn't have b64decode filter.
    """
    # Get the Snowflake connection
    conn = BaseHook.get_connection("snowflake_default")
    extra = json.loads(conn.extra) if conn.extra else {}

    # Decode the base64-encoded private key
    private_key_b64 = extra.get("private_key_content", "")
    private_key = (
        base64.b64decode(private_key_b64).decode("utf-8") if private_key_b64 else ""
    )

    return {
        "SNOWFLAKE_ACCOUNT": extra.get("account", ""),
        "SNOWFLAKE_USER": conn.login or "",
        # "SNOWFLAKE_PRIVATE_KEY_FILE_PWD": conn.password or "",
        "SNOWFLAKE_ROLE": extra.get("role", ""),
        "SNOWFLAKE_WAREHOUSE": extra.get("warehouse", ""),
        "SNOWFLAKE_DATABASE": extra.get("database", ""),
        "SNOWFLAKE_SCHEMA": conn.schema or "",
        "SNOWFLAKE_PRIVATE_KEY_file": private_key,
        "DBT_PROFILES_DIR": "/opt/airflow/dbt_sf",
        "DBT_PROJECT_DIR": "/opt/airflow/dbt_sf",
        "PATH": "/home/airflow/.local/bin:" + os.environ.get("PATH", ""),
    }


@dag(
    dag_id="capstone_dbt_orchestration",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["capstone", "dbt", "orchestration"],
    max_active_runs=1,
    description="Simple dbt orchestration using bash commands - matches capstone implementation",
)
def capstone_dbt_orchestration():
    """
    ### Capstone dbt Orchestration DAG

    This DAG follows the actual capstone implementation:
    1. Uses @task.bash for dbt execution
    2. Passes Snowflake environment variables from Airflow connections
    3. Simple dbt build command execution
    """

    @task.bash(env={**get_dbt_snowflake_env_vars()})
    def dbt_build() -> str:
        """
        Run dbt build using bash command.
        This matches exactly what's in the capstone project.
        """
        return "cd dbt_project && dbt build"

    # Execute the task
    dbt_build()


# Create the DAG instance
capstone_dbt_orchestration()
