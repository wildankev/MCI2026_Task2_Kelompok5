from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "mmds_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "orders_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    description="Daily Orders API -> Spark -> ClickHouse pipeline",
    tags=["orders", "pipeline", "clickhouse"],
) as dag:
    fetch_orders = BashOperator(
        task_id="fetch_orders",
        bash_command=(
            "python /opt/airflow/dags/scripts/fetch_orders_stream.py"
        ),
    )

    process_orders = BashOperator(
        task_id="process_orders",
        bash_command=(
            "python /opt/airflow/dags/scripts/process_orders_spark.py "
            "--stage"
        ),
    )

    load_to_clickhouse = BashOperator(
        task_id="load_to_clickhouse",
        bash_command=(
            "python /opt/airflow/dags/scripts/process_orders_spark.py "
            "--load"
        ),
    )

    fetch_orders >> process_orders >> load_to_clickhouse
