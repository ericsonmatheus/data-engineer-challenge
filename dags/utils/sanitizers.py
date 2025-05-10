import logging
import os

import pandas as pd
from airflow.exceptions import AirflowException
from utils.config import DATA_PATH

logger = logging.getLogger(__name__)

DATA_SALES_PATH = f"{DATA_PATH}/raw_data/sales/{{execution_date}}/sales_data.parquet"
DATA_EMPLOYEES_RAW_PATH = (
    f"{DATA_PATH}/raw_data/employees/{{execution_date}}/employees_data.parquet"
)
DATA_CATEGORIES_RAW_PATH = (
    f"{DATA_PATH}/raw_data/categories/{{execution_date}}/categories_data.parquet"
)

DATA_EMPLOYEES_SANITIZED_PATH = (
    f"{DATA_PATH}/staging_data/employees/{{execution_date}}/employees_sanitized.parquet"
)
DATA_CATEGORIES_SANITIZED_PATH = f"{DATA_PATH}/staging_data/categories/{{execution_date}}/categories_sanitized.parquet"


def sanitize_categories_data(execution_date: str) -> None:
    """
    Cleans and standardizes the categories DataFrame, automatically applying fixes
    for common issues found in the data.

    Args:
        execution_date (str): Execution date in the format 'YYYY-MM-DD'.
    """
    categories_data = pd.read_parquet(
        DATA_CATEGORIES_RAW_PATH.format(execution_date=execution_date)
    )

    categories = categories_data.copy()

    expected_columns = ["id", "nome_categoria"]

    # Check if the expected columns are present in the DataFrame
    for col in expected_columns:
        if col not in categories.columns:
            raise AirflowException(
                f"Missing column {col} in categories data. Please check the data source."
            )

    # Filter the DataFrame to keep only the expected columns
    categories = categories[expected_columns]

    try:
        # Convert the 'id' column to numeric, coercing errors to NaN
        categories["id"] = pd.to_numeric(categories["id"], errors="coerce")
    except Exception as e:
        raise AirflowException(f"Error converting 'id' column to numeric: {str(e)}")

    # Convert the 'nome_categoria' column to string and handle common issues
    categories["nome_categoria"] = categories["nome_categoria"].astype(str)
    # Replace common problematic values with empty strings
    categories["nome_categoria"] = categories["nome_categoria"].replace(
        ["None", "nan", "NaN", "null", "NULL"], ""
    )
    # Remove leading/trailing whitespace
    categories["nome_categoria"] = categories["nome_categoria"].str.strip()

    # Remove duplicates based on the 'id' column
    duplicate_count = categories.duplicated(subset=["id"], keep="first").sum()
    if duplicate_count > 0:
        logger.warning(
            f"Removing {duplicate_count} duplicate records based on ID from category data."
        )
        categories = categories.drop_duplicates(subset=["id"], keep="first")

    # Remove rows with null values
    categories = categories.dropna()

    categories.columns = ["id", "category_name"]

    # Create the directory if it doesn't exist
    output_dir = f"{DATA_PATH}/staging_data/categories/{execution_date}"
    os.makedirs(output_dir, exist_ok=True)

    output_file_path = f"{output_dir}/categories_sanitized.parquet"
    categories.to_parquet(output_file_path, index=False)

    logger.info(f"Categories data successfully extracted. Saved to {output_file_path}")


def sanitize_employees_data(execution_date: str) -> None:
    """
    Function to process and clean a DataFrame by applying various validations and transformations.

    Args:
        execution_date (str): Execution date in the format 'YYYY-MM-DD'.
    """
    employees_data = pd.read_parquet(
        DATA_EMPLOYEES_RAW_PATH.format(execution_date=execution_date)
    )

    employees = employees_data.copy()

    expected_columns = ["id", "name"]

    # Check if the expected columns are present in the DataFrame
    for col in expected_columns:
        if col not in employees.columns:
            raise AirflowException(
                f"Missing column {col} in employees data. Please check the data source."
            )

    # Filter the DataFrame to keep only the expected columns
    employees = employees[expected_columns]

    try:
        # Convert the 'id' column to numeric, coercing errors to NaN
        employees["id"] = pd.to_numeric(employees["id"], errors="coerce")
    except Exception as e:
        raise AirflowException(f"Error converting 'id' column to numeric: {str(e)}")

    # Convert the 'name' column to string and handle common issues
    employees["name"] = employees["name"].astype(str)
    # Replace common problematic values with empty strings
    employees["name"] = employees["name"].replace(
        ["None", "nan", "NaN", "null", "NULL"], ""
    )
    # Remove leading/trailing whitespace
    employees["name"] = employees["name"].str.strip()

    # Remove duplicates based on the 'id' column
    duplicate_count = employees.duplicated(subset=["id"], keep="first").sum()
    if duplicate_count > 0:
        logger.warning(
            f"Removing {duplicate_count} duplicate records based on ID from employee data."
        )
        employees = employees.drop_duplicates(subset=["id"], keep="first")

    # Remove rows with null values
    employees = employees.dropna()

    # Create the directory if it doesn't exist
    output_dir = f"{DATA_PATH}/staging_data/employees/{execution_date}"
    os.makedirs(output_dir, exist_ok=True)

    output_file_path = f"{output_dir}/employees_sanitized.parquet"
    employees.to_parquet(output_file_path, index=False)

    logger.info(f"Employees data successfully extracted. Saved to {output_file_path}")


def sanitize_sales_data(execution_date):
    """
    Function to process and clean a sales DataFrame by applying specific transformations
    to sales, employee, and category data.

    Args:
        execution_date (str): Execution date in the format 'YYYY-MM-DD'.
    """
    sales_data = pd.read_parquet(DATA_SALES_PATH.format(execution_date=execution_date))
    employees = pd.read_parquet(
        DATA_EMPLOYEES_SANITIZED_PATH.format(execution_date=execution_date)
    )
    categories = pd.read_parquet(
        DATA_CATEGORIES_SANITIZED_PATH.format(execution_date=execution_date)
    )

    sales = sales_data.copy()

    expected_columns = [
        "id_venda",
        "id_funcionario",
        "id_categoria",
        "data_venda",
        "venda",
    ]

    # Check if the expected columns are present in the DataFrame
    for col in expected_columns:
        if col not in sales.columns:
            raise AirflowException(
                f"Missing column {col} in sales data. Please check the data source."
            )

    # Filter the DataFrame to keep only the expected columns
    sales = sales[expected_columns]

    # Remove duplicates based on the 'id_venda' column
    duplicate_count = sales.duplicated(subset=["id_venda"], keep="first").sum()
    if duplicate_count > 0:
        logger.warning(
            f"Removing {duplicate_count} duplicate records based on ID from sales data."
        )
        sales = sales.drop_duplicates(subset=["id_venda"], keep="first")

    try:
        sales["data_venda"] = pd.to_datetime(sales["data_venda"])
    except Exception as e:
        raise AirflowException(
            f"Error converting 'data_venda' column to datetime: {str(e)}"
        )

    # Apply 0 when the value is negative
    sales.loc[sales["venda"] < 0, "venda"] = 0

    # Verify if there are employee IDs that do not exist
    expected_employees = list(employees["id"])
    invalid_funcionarios = sales[~sales["id_funcionario"].isin(expected_employees)]
    if not invalid_funcionarios.empty:
        raise AirflowException(
            f"Invalid employee IDs found in sales data: {invalid_funcionarios['id_funcionario'].unique().tolist()}"
        )

    # Verify if there are category IDs that do not exist
    expected_categorias = list(categories["id"])
    invalid_categorias = sales[~sales["id_categoria"].isin(expected_categorias)]
    if not invalid_categorias.empty:
        raise AirflowException(
            f"Invalid category IDs found in sales data: {invalid_categorias['id_categoria'].unique().tolist()}"
        )

    # Remove rows with null values
    sales = sales.dropna()

    sales.columns = ["id", "employee_id", "category_id", "sale_date", "sale_value"]

    # Create the directory if it doesn't exist
    output_dir = f"{DATA_PATH}/staging_data/sales/{execution_date}"
    os.makedirs(output_dir, exist_ok=True)

    output_file_path = f"{output_dir}/sales_sanitized.parquet"
    sales.to_parquet(output_file_path, index=False)

    logger.info(f"Sales data successfully extracted. Saved to {output_file_path}")
