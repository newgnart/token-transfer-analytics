"""
### Capstone dbt Orchestration DAG
A simple dbt orchestration DAG that matches the actual capstone implementation.
Uses @task.bash with dbt build command, following the capstone pattern.

This DAG demonstrates:
- Simple dbt integration using bash commands
- Environment variable configuration from Airflow connections
- The recommended 1 task = 1 dbt pipeline approach
"""

from airflow.decorators import dag, task
import pendulum


@dag(
    dag_id="hello_world",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["capstone", "dbt", "orchestration"],
    max_active_runs=1,
    description="Hello World DAG",
)
@task
def hello_world():
    """
    ### Hello World DAG

    This DAG prints "Hello World" to the console.
    """

    print("Hello World")


# Create the DAG instance
hello_world()
