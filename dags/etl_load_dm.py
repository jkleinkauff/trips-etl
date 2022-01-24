from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    "etl-load-dm-dag",
    default_args=default_args,
    schedule_interval="*/60 * * * *",
    catchup=False,
) as dag:
    start_dag = DummyOperator(task_id="start_dag")

    t_generate_data = DockerOperator(
        task_id="task-generate-dm-csv-data",
        image="etl-csv",
        # container_name="task_ingest_convert_csv",
        api_version="auto",
        auto_remove=True,
        command=["spark-submit", "src/fill-dm/generate-import.py"],
        # docker_url="tcp://docker-proxy:2375",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source=os.getenv("DATA_DIR"),
                target="/app/src/etl_data",
                type="bind",
            ),
            Mount(
                source=os.getenv("DAGS_DIR"),
                target="/app/src/etl_data/dags",
                type="bind",
            ),
        ],
    )

    # truncate all - certainly not for production, would do a SCD for dim and facts

    t_truncate_all = PostgresOperator(
        task_id="truncate_all_tables_data",
        sql="""
                TRUNCATE f_trips_staging, f_trips, d_datasource, d_date, d_region RESTART IDENTITY;
            """,
    )

    t_load_d_datasource = BashOperator(
        task_id="load_d_datasource",
        bash_command="cd $AIRFLOW_HOME'/dags'"
        "&& ls"
        "&& f=$(ls data/d_datasource.csv/*.csv| head -1)"
        "&& PGPASSWORD=password psql --host host.docker.internal --port 5433 --username postgres -d postgres -c '''\copy d_datasource(datasource) FROM '$f' '' WITH csv; ' ",
    )

    t_load_d_region = BashOperator(
        task_id="load_d_region",
        bash_command="cd $AIRFLOW_HOME'/dags'"
        "&& ls"
        "&& f=$(ls data/d_region.csv/*.csv| head -1)"
        "&& PGPASSWORD=password psql --host host.docker.internal --port 5433 --username postgres -d postgres -c '''\copy d_region(region_name) FROM '$f' '' WITH csv; ' ",
    )

    t_load_d_date = BashOperator(
        task_id="load_d_date",
        bash_command="cd $AIRFLOW_HOME'/dags'"
        "&& ls"
        "&& f=$(ls data/d_date.csv/*.csv| head -1)"
        "&& PGPASSWORD=password psql --host host.docker.internal --port 5433 --username postgres -d postgres -c '''\copy d_date(date,year,month,day) FROM '$f' '' WITH csv; ' ",
    )

    t_load_f_trips_staging = BashOperator(
        task_id="load_f_trips_staging",
        bash_command="cd $AIRFLOW_HOME'/dags'"
        "&& ls"
        "&& f=$(ls data/f_trips_staging.csv/*.csv| head -1)"
        "&& PGPASSWORD=password psql --host host.docker.internal --port 5433 --username postgres -d postgres "
        " -c '''\copy f_trips_staging(origin_coord_x,origin_coord_y,destination_coord_x,destination_coord_y,date,region,datasource, business_key) FROM '$f' '' WITH csv; ' ",
    )

    t_merge_f_trips = PostgresOperator(
        task_id="merge_f_trips",
        sql="""
                insert into f_trips(origin, destination, sk_region, sk_datasource, sk_date, sk_business)
                select
                    stg.origin_coord_point
                    , stg.destination_coord_point
                    , region.id 
                    , datasource.id 
                    , date.id
                    , stg.business_key
                from f_trips_staging as stg
                join d_region as region
                    on stg.region = region.region_name 
                join d_datasource as datasource
                    on stg.datasource = datasource.datasource
                join d_date as date
                    on stg."date" = date.date
                where stg.business_key not in (
                    select sk_business from f_trips
                )
            """,
    )

    start_dag >> t_truncate_all >> t_generate_data 
    t_generate_data >> t_load_d_datasource >> t_load_f_trips_staging
    t_generate_data >> t_load_d_region >> t_load_f_trips_staging
    t_generate_data >> t_load_d_date >> t_load_f_trips_staging
    t_load_f_trips_staging >> t_merge_f_trips