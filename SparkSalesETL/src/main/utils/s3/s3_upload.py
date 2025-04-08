import traceback
import datetime
import os

class UploadToS3:
    def __init__(self,s3_client):
        self.s3_client = s3_client

    def upload_to_s3(self,s3_directory,s3_bucket,local_file_path):
        s3_prefix = f"{s3_directory}"
        try:
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    s3_key = f"{s3_prefix}/{file}"
                    self.s3_client.upload_file(local_file_path, s3_bucket, s3_key)
            return f"Data Successfully uploaded in {s3_directory} data mart "
        except Exception as e:
            print(f"Error uploading file : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e