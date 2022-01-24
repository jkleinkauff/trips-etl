import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="etl-dm-dll",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    # create_pet_table = PostgresOperator(
    #     task_id="create_pet_table",
    #     sql="sql/pet_schema.sql",
    # )

    d_region = PostgresOperator(
        task_id="create_d_region",
        sql="sql/d_region.sql",
    )

    d_datasource = PostgresOperator(
        task_id="create_d_datasource",
        sql="sql/d_datasource.sql",
    )

    d_date = PostgresOperator(
        task_id="create_d_date",
        sql="sql/d_date.sql",
    )

    f_trips_staging = PostgresOperator(
        task_id="create_f_trips_staging",
        sql="sql/f_trips_staging.sql",
    )

    f_trips = PostgresOperator(
        task_id="create_f_trips",
        sql="sql/f_trips.sql",
    )


    d_region >> f_trips_staging >> f_trips
    d_datasource >> f_trips_staging >> f_trips
    d_date >> f_trips_staging >> f_trips