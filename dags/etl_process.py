from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "description": "Job to ingest CSV data and convert to parquet",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl-process-ingested-dag",
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:
    start_dag = DummyOperator(task_id="start_dag")

    t_data_process = DockerOperator(
        task_id="task-process-data-csv",
        image="etl-csv",
        # container_name="task_ingest_convert_csv",
        api_version="auto",
        auto_remove=True,
        # command="/bin/sleep 30",
        command=["spark-submit", "src/process/spark_process.py"],
        # docker_url="tcp://docker-proxy:2375",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source=os.getenv("DATA_DIR"), 
                target='/app/src/etl_data', 
                type='bind'
            )
        ],
    )

    start_dag >> t_data_process
