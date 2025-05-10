import os

# Paths for files and directories used in the pipeline
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(BASE_DIR, "data")

EMPLOYEES_API_CONFIG = {
    "base_url": os.environ.get("EMPLOYEES_API_URL"),
}

CATEGORIES_URL_PARQUET = os.environ.get("CATEGORIES_URL_PARQUET")
