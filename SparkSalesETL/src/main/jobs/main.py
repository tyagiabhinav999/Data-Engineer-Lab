import os
import shutil
import sys
from pathlib import Path

from src.main.utils.s3.s3_move import move_s3_to_s3
from src.main.utils.spark.spark_session import spark_session
from src.main.config import config
from src.main.utils.security.encryption import AWSCredentialsManager
from src.main.utils.s3.s3_client_object import S3ClientProvider
from src.main.utils.s3.s3_reader import S3Reader
from src.main.utils.s3.s3_downloader import S3Downloader
from src.main.utils.db.mysql_connector import MySQLConnector

import logging
from src.main.config.logging_config import setup_logging

# Setup logging
setup_logging()

# Get logger for this module
logger = logging.getLogger('main')
logger.info("Logging setup complete (hopefully using YAML config now).")

# Create instance of AWSCredentialsManager
manager = AWSCredentialsManager()

# Call decrypt_credentials method to get keys
aws_access_key, aws_secret_key = manager.decrypt_credentials()

# Call s3 client
s3_client_provider = S3ClientProvider(aws_access_key, aws_secret_key, 'ap-south-1')
s3_client = s3_client_provider.get_client()

# Print bucket list
response = s3_client.list_buckets() 

# Check if local directory already has a file
# If file is there, check if same file is in the staging area with status A (Active)
# If yes, don't delete it, try to re-run
# Else give an error and do not process the next file
csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]
# Connect to MySQL
db = MySQLConnector(config.db_config)
db.connect()
connection = db.get_connection()
cursor = db.get_cursor()

# Initialize Schema
db.initialize_schema(config.sql_script_path)


# query = f'select distinct file_name'\
#         f'from sparksales.product_staging_table'\
#         f'where file_name in ({str(csv_files)[1:-1]} and status="I"'
# cursor.execute(query)
# data = cursor.fetchall()
# print(data)

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_abs_file_path = s3_reader.list_files(s3_client, config.s3_bucket, folder_path)
    logger.info(f'Absolute path on s3 bucket for csv file: {s3_abs_file_path}')
    if not s3_abs_file_path:
        logger.debug(f'No files available at {s3_abs_file_path}')
        raise Exception('No data to process')
except Exception as e:
    logger.error(f'Exited with Error: {e}')
    raise e


prefix = f's3://{config.s3_bucket}/'
file_paths = [url[len(prefix):] for url in s3_abs_file_path]

try:
    s3_downloader = S3Downloader(config.s3_bucket, s3_client, config.local_directory)
    s3_downloader.download_files(file_paths)
except Exception as e:
    print(f'File download error: {e}')
    sys.exit()

files_downloaded_from_s3 = [f.name for f in Path(config.local_directory).iterdir()]
logger.info(f'List of files downloaded from S3: {files_downloaded_from_s3}')
csv_downloaded_from_s3 = [f.name for f in Path(config.local_directory).iterdir() if f.suffix=='.csv']
logger.info(f'List of CSV files downloaded from S3 and needs to be processes: {csv_downloaded_from_s3}')

if csv_downloaded_from_s3:
    csv_files = []
    error_files = []
    for files in csv_downloaded_from_s3:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(Path(config.local_directory, files)))
        else:
            error_files.append(os.path.abspath(Path(config.local_directory, files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process.")


logger.info('*********** Starting Spark Session...*************')
spark = spark_session()
logger.info('*********** Spark Session Created...*************')

# check the required columns in the schema for csv files,
# if required columns not found, move to error_files
# else union all into one dataframe
files_to_be_processed = []
for file in csv_files:
    file_schema = spark.read.format('csv')\
                    .option('header', 'true') \
                    .load(file).columns
    logger.info(f'Schema for {file} in {file_schema}')
    logger.info(f'Mandatory columns schema is {config.mandatory_columns}')
    missing_columns = list(set(config.mandatory_columns) - set(file_schema))
    logger.info(f'Missing columns are: {missing_columns}')

    if missing_columns:
        error_files.append(file)
    else:
        logger.info(f'No missing columns found in: {file}')
        files_to_be_processed.append(file)

logger.info(f'******** List of Files needs to be Processed : {files_to_be_processed} ************ ')
logger.info(f'******** List of Error Files: {error_files} ********************')
logger.info(f'Moving Error Files to Error Directory...')

error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if Path(file_path).exists():
            file_name = Path(file_path).name
            destination_path = Path(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f'Moved Error file {file_name} from S3 file path to {destination_path}')

            source_prefix = f'{config.s3_source_directory}/'
            destination_prefix = f'{config.s3_error_directory}/'
            print(file_path)
            msg = move_s3_to_s3(s3_client, config.s3_bucket, source_prefix, destination_prefix, Path(file_path).name)
            logger.info(msg)
        else:
            logger.error(f'{file_path} does not exists...')

else:
    logger.info(f'No Error files available to move.')


'''
Additional Columns needs to be taken care of.

Before running the process, staging table must be updated with proper status.
status(New 'N', Failed 'F', Ingested 'I', Completed 'C')
'''
# --- JDBC Connection Details for Spark ---
jdbc_url = f"jdbc:mysql://{config.db_config['host']}:{config.db_config.get('port', 3306)}/{config.db_config['database']}"
connection_properties = {
    "user": config.db_config['user'],
    "password": config.db_config['password'],
    "driver": "com.mysql.cj.jdbc.Driver"
}
# if source_csv_files:
#     try:
#         # Create placeholders string like '%s, %s, %s'
#         placeholders = ', '.join(['%s'] * len(source_csv_files))
#         # Corrected query structure and parameterization
#         query = f"SELECT DISTINCT file_name FROM sparksales.product_staging_table WHERE file_name IN ({placeholders}) AND status = %s"
#         # Combine file names and the status for parameters
#         params = source_csv_files + ['Completed'] # Assuming 'Completed' marks success
#         cursor.execute(query, params)
#         processed_files = {row[0] for row in cursor.fetchall()} # Set for efficient lookup
#     except Exception as e:
#         print(f"Error checking processed files: {e}")
#         processed_files = set() # Or handle error differently
# else:
#     processed_files = set()

# files_to_process = [f for f in source_csv_files if f not in processed_files]




# Configure JDBC connection properties
# jdbc_url = f"jdbc:mysql://{config.db_config['host']}:{config.db_config.get('port', 3306)}/{config.db_config['database']}"
# connection_properties = {
#     "user": config.db_config['user'],
#     "password": config.db_config['password'], # Ensure this is handled securely!
#     "driver": "com.mysql.cj.jdbc.Driver" # Or your MySQL driver
# }
#
# # Add columns needed for staging table (like filename, status, load_timestamp)
# from pyspark.sql.functions import lit, current_timestamp
# df_to_write = spark_df.withColumn("file_name", lit(filename)) \
#                       .withColumn("status", lit("Staged")) \ # Or 'Ingested'
#                       .withColumn("load_timestamp", current_timestamp())
#
# # Write the DataFrame to the MySQL table
# df_to_write.write.jdbc(
#     url=jdbc_url,
#     table="sparksales.product_staging_table", # Your table name
#     mode="append", # Add data, don't overwrite existing table
#     properties=connection_properties
# )
# print(f"Data from {filename} loaded into staging table.")





# try:
#     insert_query = """
#         INSERT INTO sparksales.product_staging_table
#         (file_name, file_location, created_at, updated_at, status)
#         VALUES (%s, %s, NOW(), NOW(), %s)
#         ON DUPLICATE KEY UPDATE status = %s, updated_at = NOW()
#     """
#     # Status 'I' = Ingesting/In Progress
#     params = (filename, config.local_directory, 'I', 'I')
#     cursor.execute(insert_query, params)
#     connection.commit() # Commit the status update
#     print(f"Marked {filename} as 'Ingesting' in control table.")
# except Exception as e:
#      print(f"Error marking {filename} in control table: {e}")
#      connection.rollback() # Rollback if marking fails
#      continue # Skip this file if we can't mark it
#
# # --- Now process the actual data ---
# try:
#     file_path = os.path.join(config.local_directory, filename)
#
#     # ii. Read CSV Data with Spark
#     spark_df = spark.read.csv(file_path, header=True, inferSchema=True) # Add options
#
#     # iii. Perform Transformations with Spark (if any needed before final load)
#     # transformed_df = spark_df.select(...) # Example
#
#     # iv. Write Transformed Data to FINAL Destination Table
#     # THIS is where the actual CSV data goes. Assume you have another table,
#     # e.g., 'sparksales.sales_fact_table' designed to hold the sales data.
#     final_table_name = "sparksales.sales_fact_table" # Replace with your actual destination
#     transformed_df.write.jdbc(
#          url=jdbc_url,
#          table=final_table_name,
#          mode="append",
#          properties=connection_properties
#     )
#     print(f"Data from {filename} successfully loaded into {final_table_name}.")
#
#     # v. Update Control Table to 'Completed' on Success
#     update_query = "UPDATE sparksales.product_staging_table SET status = 'C', updated_at = NOW() WHERE file_name = %s AND status = 'I'"
#     cursor.execute(update_query, (filename,))
#     connection.commit()
#     print(f"Marked {filename} as 'Completed' in control table.")
#
# except Exception as e:
#     print(f"ERROR processing file {filename}: {e}")
#     connection.rollback() # Rollback any partial commits for this file
#     # vi. Update Control Table to 'Error' on Failure
#     try:
#         error_update_query = "UPDATE sparksales.product_staging_table SET status = 'E', updated_at = NOW() WHERE file_name = %s"
#         cursor.execute(error_update_query, (filename,))
#         connection.commit()
#         print(f"Marked {filename} as 'Error' in control table.")
#     except Exception as update_e:
#         print(f"Failed to mark {filename} as error: {update_e}")
#         connection.rollback()



