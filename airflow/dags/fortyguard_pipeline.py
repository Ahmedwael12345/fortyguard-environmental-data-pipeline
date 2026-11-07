from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"
PYTHON = "/opt/airflow/project-venv/bin/python"

default_args = {
    "owner": "fortyguard",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fortyguard_hourly_pipeline",
    description="Hourly FortyGuard environmental data pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 11, 7, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["fortyguard", "data-engineering", "hourly"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_fortyguard_api",
        bash_command=f"""
            set -e
            cd {PROJECT_DIR}
            {PYTHON} src/ingest.py
        """,
        execution_timeout=timedelta(minutes=45),
    )

    transform = BashOperator(
        task_id="transform_bronze_to_silver",
        bash_command=f"""
            set -e
            cd {PROJECT_DIR}
            {PYTHON} src/transform_silver.py
        """,
        execution_timeout=timedelta(minutes=30),
    )

    validate = BashOperator(
        task_id="validate_silver",
        bash_command=f"""
            set -e
            cd {PROJECT_DIR}
            {PYTHON} src/validate_silver.py
        """,
        execution_timeout=timedelta(minutes=10),
    )

    upload = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"""
            set -e
            cd {PROJECT_DIR}
            {PYTHON} src/upload_to_s3.py
        """,
        execution_timeout=timedelta(minutes=30),
    )

    ingest >> transform >> validate >> upload
