import csv
import random
from faker import Faker
from pathlib import Path
from datetime import datetime

# Initialize Faker with Indian locale
fake = Faker('en_IN')

# Output CSV file name
csv_file = 'customers.csv'

# Define the headers for the CSV file
headers = ['first_name', 'last_name', 'address', 'pincode', 'phone_number', 'customer_joining_date']
file_location = r"C:\Users\hp\spark-sales-etl"
csv_file_path = Path(file_location, csv_file)
# Open file in write mode
with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(headers)  # Write header row

    for _ in range(25):
        first_name = fake.first_name()
        last_name = fake.last_name()
        address = 'Delhi'
        pincode = '122009'
        phone_number = '91' + ''.join([str(random.randint(0, 9)) for _ in range(8)])
        joining_date = fake.date_between_dates(
            date_start=datetime(2020, 1, 1),
            date_end=datetime(2023, 8, 20)
        ).strftime('%Y-%m-%d')

        # Write the row to the CSV
        writer.writerow([first_name, last_name, address, pincode, phone_number, joining_date])

print(f"✅ CSV file '{csv_file}' created with 25 customer records.")
