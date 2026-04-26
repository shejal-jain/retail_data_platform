import csv
import os
from datetime import datetime,UTC
import random

def generate_customers_csv(file_path, n=10):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    fieldnames = ["customer_id","name","email","city","signup_date"]
    cities = ["Bangalore","Mumbai","Delhi","Hyderabad"]

    with open(file_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for i in range (1,n+1):
            writer.writerow({
                "customer_id":i,
                "name": f"Customer_{i}",
                "email":f"Customer{i}@example.com",
                "city":random.choice(cities),
                "signup_date":datetime.now(UTC).date().isoformat()
            }
            )
        
    return file_path