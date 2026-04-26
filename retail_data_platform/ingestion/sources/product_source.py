import csv
import os
import random
from datetime import datetime,UTC

def generate_products_csv(file_path, n=10):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    field_names = ["product_id","product_name","category","price","created_at"]
    categories = ["Electronics","Clothing","Home","Books"]

    with open(file_path, mode="w", newline='') as file:
        writer = csv.DictWriter(file,fieldnames=field_names)
        writer.writeheader()

        for i in range(1, n + 1):
            writer.writerow({
                "product_id": i,
                "product_name": f"Product_{i}",
                "category": random.choice(categories),
                "price": round(random.uniform(100, 5000), 2),
                "created_at": datetime.now(UTC).isoformat()
            })

    return file_path