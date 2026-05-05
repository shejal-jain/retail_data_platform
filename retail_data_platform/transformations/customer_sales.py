import os
import json

from retail_data_platform.config.config import BASE_DATA_LAKE_PATH
from retail_data_platform.ingestion.utils.logger import get_logger

logger = get_logger("customer_sales")

#Reads the latest data file from your data lake
def load_latest_file(base_path, entity):
    entity_path = os.path.join(base_path,entity)

    dates = sorted(os.listdir(entity_path))
    latest_date = dates[-1]
    date_path = os.path.join(entity_path, latest_date)

    files = sorted(os.listdir(date_path))
    #Picks latest run file:
    latest_file = files[-1]

    file_path = os.path.join(date_path,latest_file)

    #Loads data into Python list
    with open(file_path, "r") as f:
        data = json.load(f)
    
    logger.info(f"Loaded {entity} from {file_path}")

    return data


def compute_customer_sales(customers, orders):
    """
    Return list of:
    {
        customer_id,
        customer_name,
        total_orders,
        total_amount
    }
    """

    result = {}
    invalid_orders = 0
    valid_orders = 0

    for c in customers:
        cid = str(c["customer_id"])
        result[cid] = {
            "customer_id": "cid",
            "customer_name": c.get("name"),
            "total_orders": 0,
            "total_amount": 0
        }
    
    #Loop over each order
    for o in orders:
      try:
        cid = str(o["customer_id"])
        price = o.get("price", 0)
        quantity = o.get("quantity", 0)
     
      #Validation checks
        if price < 0 or quantity <= 0:
            invalid_orders += 1
            logger.warning(f"Invalid order skipped: {o}")
            continue

        amount = price * quantity

        #Check if customer exists
        if cid in result:
            #Update
            result[cid]["total_orders"] += 1
            result[cid]["total_amount"] += amount

      except Exception as e:
          invalid_orders += 1
          logger.error(f"Error processing order: {o} | {str(e)}")

    #Convert dictionary → list of records
    logger.info(f"Valid orders processed: {valid_orders}")
    logger.info(f"Invalid orders skipped: {invalid_orders}")

    return list(result.values()), valid_orders, invalid_orders


def save_analytics(data, run_id, logical_date):
    output_path = os.path.join(
        BASE_DATA_LAKE_PATH,
        "analytics",
        "customer_sales",
        logical_date.strftime("%Y-%m-%d")
    )

    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, f"customer_sales_{run_id}.json")

    with open(file_path,"w") as f:
        json.dump(data,f,indent=2)

    logger.info(f"Saved analytics to {file_path}")
    return file_path


def save_quality_report(valid_orders, invalid_orders, run_id, logical_date):
    report_path = os.path.join(
        BASE_DATA_LAKE_PATH,
        "analytics",
        "data_quality",
        logical_date.strftime("%Y-%m-%d")
    )

    os.makedirs(report_path, exist_ok=True)

    file_path = os.path.join(report_path, f"dq_customer_sales_{run_id},json")

    report = {
        "run_id": run_id,
        "valid_orders": valid_orders,
        "invalid_orders": invalid_orders,
        "invalid_percentage": (
            invalid_orders/(valid_orders + invalid_orders) 
            if (valid_orders + invalid_orders) > 0 else 0
        )
    }

    with open(file_path,"w") as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Saved data quality report: {file_path}")


def run_customer_sales(run_id, logical_date):
    logger.info(f"Customer Sales Transformation START | run_id={run_id}")
    
    customers = load_latest_file(BASE_DATA_LAKE_PATH, "customers")
    orders = load_latest_file(BASE_DATA_LAKE_PATH, "orders")

    output, valid_orders, invalid_orders = compute_customer_sales(customers, orders)

    file_path = save_analytics(output, run_id, logical_date)
    save_quality_report(valid_orders, invalid_orders, run_id, logical_date)

    logger.info("Customer Sales Transformation End")

    return file_path

    


