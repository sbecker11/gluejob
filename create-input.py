import os
import csv
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load properties from .env file
load_dotenv()

# Access the properties from the .env file
INPUT_CSV_FILE = os.getenv("INPUT_CSV_FILE")

def create_input():
    # Define the start and end datetime range
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 2, 10)

    # Generate 10 random datetime values within the specified range
    random_dates = [start_date + timedelta(days=random.randint(0, (end_date - start_date).days)) for _ in range(10)]

    # Generate random data for each row
    data = []
    for i in range(10):
        order_id = i + 1
        user_id = random.randint(1000, 9999)
        amount_usd = round(random.uniform(10.0, 100.0), 2)
        datetime_str = random_dates[i].strftime("%Y-%m-%dT%H:%M:%S")

        data.append([order_id, user_id, amount_usd, datetime_str])

    # Write data to a CSV file
    with open(INPUT_CSV_FILE, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['order_id', 'user_id', 'amount_usd', 'datetime'])
        writer.writerows(data)

    print(f"CSV file {INPUT_CSV_FILE}' has been created.")

if __name__ == '__main__':
    create_input()