# Central configuration file
import os

# Base paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BASE_DATA_LAKE_PATH = os.path.join(BASE_DIR, "data_lake", "raw")
BASE_SOURCE_PATH = os.path.join(BASE_DIR, "data_source")

# Entity constants (avoid hardcoding strings)
ORDERS = "orders"
CUSTOMERS = "customers"
PRODUCTS = "products"

# Allowed entities (validation)
ALLOWED_ENTITIES = [
    "customers",
    "products",
    "orders",
    "customers_staging",
    "products_staging"
]