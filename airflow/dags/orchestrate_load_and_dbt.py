"""
Master Orchestration DAG: Log Collection → dbt Transformation

This DAG orchestrates the complete data pipeline:
1. Trigger and wait for collect_and_load_logs_dag to complete
2. Trigger dbt_sf transformation

Uses TriggerDagRunOperator to chain DAG execution while keeping them modular.

IMPORTANT: This is the ONLY DAG that should be scheduled.
- collect_and_load_logs_dag: schedule=None (triggered by this DAG)
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
    1. Log collection and loading completes successfully
    2. dbt transformations run on the freshly loaded data

    The pipeline stops if log collection fails, preventing dbt from running on stale data.
    """

    # Step 1: Trigger log collection DAG and wait for completion
    trigger_log_collection = TriggerDagRunOperator(
        task_id="trigger_log_collection",
        trigger_dag_id="collect_and_load_logs_dag",
        wait_for_completion=True,  # Blocks until DAG completes
        poke_interval=30,  # Check every 30 seconds
        reset_dag_run=True,  # Reset if already running
    )

    # Step 2: Trigger dbt transformation DAG
    trigger_dbt_transformation = TriggerDagRunOperator(
        task_id="trigger_dbt_transformation",
        trigger_dag_id="dbt_sf",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    # Define pipeline flow: Log Collection → dbt Transform
    # No sensor needed - TriggerDagRunOperator already waits for completion
    trigger_log_collection >> trigger_dbt_transformation


# Create DAG instance
orchestrate_load_and_dbt()
