"""
Master Orchestration DAG: Log Collection → Load to Snowflake → dbt Transformation

This DAG orchestrates the complete data pipeline:
1. Trigger and wait for collect_logs to complete (extract logs from HyperSync)
2. Trigger and wait for load_logs_sf to complete (load parquet to Snowflake)
3. Trigger dbt_sf transformation

Uses TriggerDagRunOperator to chain DAG execution while keeping them modular.

IMPORTANT: This is the ONLY DAG that should be scheduled.
- collect_logs: schedule=None (triggered by this DAG)
- load_logs_sf: schedule=None (triggered by this DAG)
- dbt_sf: schedule=None (triggered by this DAG)
"""

import pendulum
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="orchestrate_load_and_dbt",
    schedule="0 0 * * *",  # Daily at 2 AM UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["orchestration", "master", "logs", "dbt"],
    max_active_runs=1,
    description="Master DAG: Orchestrates log collection followed by dbt transformation",
)
def orchestrate_load_and_dbt():
    """
    ### Master Pipeline Orchestration

    This DAG ensures that:
    1. Log collection completes successfully (extract from HyperSync)
    2. Collected logs are loaded to Snowflake
    3. dbt transformations run on the freshly loaded data

    The pipeline stops if any step fails, preventing downstream tasks from running on stale data.
    """

    # Step 1: Trigger log collection DAG and wait for completion
    trigger_log_collection = TriggerDagRunOperator(
        task_id="trigger_log_collection",
        trigger_dag_id="collect_logs",
        wait_for_completion=True,  # Blocks until DAG completes
        poke_interval=30,  # Check every 30 seconds
        reset_dag_run=True,  # Reset if already running
    )

    # Step 2: Trigger load to Snowflake DAG and wait for completion
    trigger_load_snowflake = TriggerDagRunOperator(
        task_id="trigger_load_snowflake",
        trigger_dag_id="load_logs_sf",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Step 3: Trigger dbt transformation DAG
    trigger_dbt_transformation = TriggerDagRunOperator(
        task_id="trigger_dbt_transformation",
        trigger_dag_id="dbt_sf",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Define pipeline flow: Log Collection → Load to Snowflake → dbt Transform
    trigger_log_collection >> trigger_load_snowflake >> trigger_dbt_transformation


# Create DAG instance
orchestrate_load_and_dbt()
