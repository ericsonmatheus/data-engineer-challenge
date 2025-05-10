import logging
import os

import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.config import CATEGORIES_URL_PARQUET, DATA_PATH, EMPLOYEES_API_CONFIG

logger = logging.getLogger(__name__)


def extract_sales_from_postgres(engine: PostgresHook, execution_date: str) -> None:
    """
    Extracts sales data from a PostgreSQL database and saves it as a Parquet file.

    Args:
        engine (PostgresHook): A connection hook to the sales database.
        execution_date (str): Execution date in the format 'YYYY-MM-DD'.
    """
    try:
        logger.info(f"Extracting sales data for the date: {execution_date}.")

        query = "SELECT * FROM venda"

        sales_df = pd.read_sql(query, engine)

        # Create the directory if it doesn't exist
        output_dir = f"{DATA_PATH}/raw_data/sales/{execution_date}"
        os.makedirs(output_dir, exist_ok=True)

        output_file_path = f"{output_dir}/sales_data.parquet"
        sales_df.to_parquet(output_file_path, index=False)

        logger.info(
            f"Sales data successfully extracted: {len(sales_df)} records saved to {output_file_path}"
        )

    except Exception as e:
        logger.error(f"Error while extracting sales data from PostgreSQL: {str(e)}")
        raise


def extract_employees_from_api(execution_date: str) -> None:
    """
    Extracts employee data from an API and saves it as a Parquet file.

    Args:
        execution_date (str): Execution date in the format 'YYYY-MM-DD'.
    """
    try:
        logger.info(
            f"Initiating employee data extraction from the API for the date: {execution_date}."
        )

        base_url = EMPLOYEES_API_CONFIG["base_url"]

        all_employees = []
        has_more = True
        employee_id = 1

        while has_more:
            url = f"{base_url}?id={employee_id}"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.text.strip()

                if data != "The argument is not correct":
                    all_employees.append({"id": employee_id, "name": data})
                    employee_id += 1
                else:
                    has_more = False
                    logger.warning(
                        f"No more data found after employee ID {employee_id-1}."
                    )
            else:
                logger.error(
                    f"Failed to retrieve data for employee ID {employee_id}. HTTP Status: {response.status_code}"
                )
                break

        if not all_employees:
            logger.warning("No employee data extracted.")
            return

        employees_df = pd.DataFrame(all_employees)

        # Create the directory if it doesn't exist
        output_dir = f"{DATA_PATH}/raw_data/employees/{execution_date}"
        os.makedirs(output_dir, exist_ok=True)

        output_file_path = f"{output_dir}/employees_data.parquet"
        employees_df.to_parquet(output_file_path, index=False)

        logger.info(
            f"Successfully extracted {len(employees_df)} employees records. Data saved to: {output_file_path}"
        )

    except Exception as e:
        logger.error(
            f"An error occurred while extracting employees data from the API: {str(e)}"
        )
        raise


def extract_categories_from_parquet(execution_date: str) -> None:
    """
    Extracts category data from a Parquet file hosted on Google Cloud Storage
    and saves it locally as a new Parquet file.

    Args:
        execution_date (str): Execution date in the format 'YYYY-MM-DD'.
    """
    try:
        logger.info(
            f"Starting category data extraction from GCS: {CATEGORIES_URL_PARQUET}"
        )

        categories_df = pd.read_parquet(CATEGORIES_URL_PARQUET, engine="pyarrow")

        # Create the directory if it doesn't exist
        output_dir = f"{DATA_PATH}/raw_data/categories/{execution_date}"
        os.makedirs(output_dir, exist_ok=True)

        output_file_path = f"{output_dir}/categories_data.parquet"
        categories_df.to_parquet(output_file_path, index=False)

        logger.info(
            f"Successfully extracted {len(categories_df)} categories records. Data saved to: {output_file_path}"
        )

    except Exception as e:
        logger.error(f"Error while extracting category data from GCS: {str(e)}")
        raise
