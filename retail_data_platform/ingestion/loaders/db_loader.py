import psycopg2
import json

def load_customer_sales_to_db(file_path):
    conn = psycopg2.connect(
        host="postgres_retail",
        database="retail_db",
        user="airflow",
        password="airflow"
    )

    cur = conn.cursor()

    with open(file_path,"r") as f:
        data = json.load(f)

    for row in data:
        cur.execute("""
            INSERT INTO customer_sales (
                customer_id,
                customer_name,
                total_orders,
                total_amount
            ) VALUES (%s, %s, %s, %s)
        """,(
            row["customer_id"],
            row["customer_name"],
            row["total_orders"],
            row["total_amount"]
        ))

    conn.commit()
    cur.close()
    conn.close()