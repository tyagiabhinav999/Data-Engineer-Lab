import traceback

class S3Reader:

    def list_files(self, s3_client, bucket_name, folder_path):
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
            if 'Contents' in response:
                files = [f"s3://{bucket_name}/{obj['key']}" for obj in response['Contents'] if
                         not obj['key'].endswith('/')]
                return files
            else:
                return []
        except Exception as e:
            error_msg = f'Error Listing files {e}'
            traceback_msg = traceback.format_exc()
            print(traceback_msg)
            raise