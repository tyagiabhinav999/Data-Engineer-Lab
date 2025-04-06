from src.main.utils.security.encrypt_decrypt import AWSCredentialsManager
from src.main.utils.s3_client_object import S3ClientProvider

# Create instance of AWSCredentialsManager
manager = AWSCredentialsManager()

# Call decrypt_credentials method to get keys
aws_access_key, aws_secret_key = manager.decrypt_credentials()

# Call s3 client
s3_client_provider = S3ClientProvider(aws_access_key, aws_secret_key, 'ap-south-1')
s3_client = s3_client_provider.get_client()

# Print bucket list
response = s3_client.list_buckets()
print(response['Buckets'])
