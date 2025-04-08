from src.main.utils.security.encryption import AWSCredentialsManager
from src.main.utils.s3.s3_client_object import S3ClientProvider
from s3_upload import UploadToS3
from src.main.config.config import s3_source_directory, s3_bucket, upload_data_file_path

# Create instance of AWSCredentialsManager
manager = AWSCredentialsManager()

# Call decrypt_credentials method to get keys
aws_access_key, aws_secret_key = manager.decrypt_credentials()

# Call s3 client
s3_client_provider = S3ClientProvider(aws_access_key, aws_secret_key, 'ap-south-1')
s3_client = s3_client_provider.get_client()

# Upload Sales data to S3
uploader = UploadToS3(s3_client)
uploader.upload_to_s3(s3_source_directory, s3_bucket, upload_data_file_path)