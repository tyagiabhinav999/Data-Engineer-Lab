# AWS Credentials
s3_bucket = 'spark-sales-etl'
s3_customer_datamart = 'customer-data-mart'
s3_sales_datamart = 'sales-data-mart'
s3_sales_partitioned_datamart = 'sales-partitioned-data-mart'
s3_source_directory = 'sales-data'
s3_error_directory = 'sales-data-error'
s3_processed_directory = 'sales-data-processed'

# MySQL Configurations
database_name = ''
url = ''
properties = {
    "user": "root",
    "password": '',
    "driver":''
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

# Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Additional Table name
inventory_table = "inventory"
orders_table = "orders"
returns_table = "returns"
supplier_table = "supplier"

# Additional Data Mart details
inventory_data_mart_table = "inventory_data_mart"
orders_data_mart_table = "orders_data_mart"
returns_data_mart_table = "returns_data_mart"
supplier_data_mart_table = "supplier_data_mart"


# File Download location
local_directory = r"C:\Users\hp\spark-sales-etl\s3_files"
customer_data_mart_local_file = r"C:\Users\hp\spark-sales-etl\customer_data_mart"
sales_team_data_mart_local_file = r"C:\Users\hp\spark-sales-etl\sales_data_mart"
sales_team_data_mart_partitioned_local_file = r"C:\Users\hp\spark-sales-etl\sales_partition_data"
error_folder_path_local = r"C:\Users\hp\spark-sales-etl\error_files"
