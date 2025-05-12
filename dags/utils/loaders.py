import logging

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.config import DATA_PATH

from .postgres_upsert_table import PostgresUpsertFactory

logger = logging.getLogger(__name__)

DATA_EMPLOYEES_SANITIZED_PATH = (
    f"{DATA_PATH}/staging_data/employees/{{execution_date}}/employees_sanitized.parquet"
)
DATA_CATEGORIES_SANITIZED_PATH = f"{DATA_PATH}/staging_data/categories/{{execution_date}}/categories_sanitized.parquet"
DATA_SALES_SANITIZED_PATH = (
    f"{DATA_PATH}/staging_data/sales/{{execution_date}}/sales_sanitized.parquet"
)


def load_categories_data(engine: PostgresHook, execution_date: str) -> None:
    categories_data = pd.read_parquet(
        DATA_CATEGORIES_SANITIZED_PATH.format(execution_date=execution_date)
    )
    upsert_method_factory = PostgresUpsertFactory()
    upsert_factor = upsert_method_factory.build(constraint="categories_pkey")
    categories_data.to_sql(
        "categories",
        con=engine,
        if_exists="append",
        index=False,
        method=upsert_factor,
    )


def load_employees_data(engine: PostgresHook, execution_date: str) -> None:
    employees_data = pd.read_parquet(
        DATA_EMPLOYEES_SANITIZED_PATH.format(execution_date=execution_date)
    )
    upsert_method_factory = PostgresUpsertFactory()
    upsert_factor = upsert_method_factory.build(constraint="employees_pkey")
    employees_data.to_sql(
        "employees",
        con=engine,
        if_exists="append",
        index=False,
        method=upsert_factor,
    )


def load_sales_data(engine: PostgresHook, execution_date: str) -> None:
    sales_data = pd.read_parquet(
        DATA_SALES_SANITIZED_PATH.format(execution_date=execution_date)
    )
    upsert_method_factory = PostgresUpsertFactory()
    upsert_factor = upsert_method_factory.build(constraint="sales_pkey")
    sales_data.to_sql(
        "sales",
        con=engine,
        if_exists="append",
        index=False,
        method=upsert_factor,
    )
