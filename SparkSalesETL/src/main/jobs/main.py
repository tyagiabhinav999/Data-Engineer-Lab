import os
import sys
from pathlib import Path
from src.main.config import config
from src.main.utils.security.encryption import AWSCredentialsManager
from src.main.utils.s3.s3_client_object import S3ClientProvider
from src.main.utils.s3.s3_reader import S3Reader
from src.main.utils.s3.s3_downloader import S3Downloader
from src.main.utils.db.mysql_connector import MySQLConnector

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
    print(f'Absolute path on s3 bucket for csv file: {s3_abs_file_path}')
    if not s3_abs_file_path:
        print(f'No files available at {s3_abs_file_path}')
        raise Exception('No data to process')
except Exception as e:
    print(f'Exited with Error: {e}')
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
print(f'List of files downloaded from S3: {files_downloaded_from_s3}')
csv_downloaded_from_s3 = [f.name for f in Path(config.local_directory).iterdir() if f.suffix=='.csv']
print(f'List of CSV files downloaded from S3: {csv_downloaded_from_s3}')




