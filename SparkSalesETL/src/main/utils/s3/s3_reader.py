import traceback

class S3Reader:

    def list_files(self, s3_client, bucket_name, folder_path):
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{folder_path}/")
            if 'Contents' in response:
                print(f"Total files available in folder {folder_path} of bucket {bucket_name}: {response}")
                files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents'] if
                         not obj['Key'].endswith('/')]

                return files
            else:
                return []
        except Exception as e:
            error_msg = f'Error Listing files {e}'
            traceback_msg = traceback.format_exc()
            print(traceback_msg)
            raise