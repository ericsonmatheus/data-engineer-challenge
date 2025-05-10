from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.extractors import (
    extract_categories_from_parquet,
    extract_employees_from_api,
    extract_sales_from_postgres,
)
from utils.sanitizers import (
    sanitize_categories_data,
    sanitize_employees_data,
    sanitize_sales_data,
)

sales_engine = PostgresHook(postgres_conn_id="db_sales").get_sqlalchemy_engine()

default_args = {
    "owner": "ericson",
    "depends_on_past": False,
    "email": ["ericson.matheus.2016@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="sales_data_pipeline_dag",
    default_args=default_args,
    description="Pipeline de dados para análise de vendas",
    schedule_interval="0 5 * * *",
    start_date=datetime(2025, 5, 9),
    catchup=False,
    tags=["sales", "data-engineering"],
    max_active_runs=1,  # Ensures only one instance of this DAG runs at any given time
) as dag:

    extract_sales_task = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_sales_from_postgres,
        op_kwargs={
            "engine": sales_engine,
            "execution_date": "{{ ds }}",
        },
    )

    extract_employees_task = PythonOperator(
        task_id="extract_employees_data",
        python_callable=extract_employees_from_api,
        op_kwargs={
            "execution_date": "{{ ds }}",
        },
    )

    extract_categories_task = PythonOperator(
        task_id="extract_categories_data",
        python_callable=extract_categories_from_parquet,
        op_kwargs={
            "execution_date": "{{ ds }}",
        },
    )

    sanitize_sales_task = PythonOperator(
        task_id="sanitize_sales_data",
        python_callable=sanitize_sales_data,
        op_kwargs={
            "execution_date": "{{ ds }}",
        },
    )

    sanitize_employees_task = PythonOperator(
        task_id="sanitize_employees_data",
        python_callable=sanitize_employees_data,
        op_kwargs={
            "execution_date": "{{ ds }}",
        },
    )

    sanitize_categories_task = PythonOperator(
        task_id="sanitize_categories_data",
        python_callable=sanitize_categories_data,
        op_kwargs={
            "execution_date": "{{ ds }}",
        },
    )

    (
        [extract_sales_task, extract_employees_task, extract_categories_task]
        >> sanitize_categories_task
        >> sanitize_employees_task
        >> sanitize_sales_task
    )
