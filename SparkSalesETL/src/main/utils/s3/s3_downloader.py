import traceback
from pathlib import Path

class S3Downloader:
    def __init__(self, bucket_name, s3_client, destination):
        self.bucket_name = bucket_name
        self.s3_client = s3_client
        self.destination = destination

    def download_files(self, list_files):
        for key in list_files:
            filename = Path(key).name
            print(f'File name is: {filename}')
            download_file_path = Path(self.destination)/filename

            try:
                self.s3_client.download_file(self.bucket_name, key, download_file_path)
            except Exception as e:
                error_message = f"Error downloading file '{key}': {str(e)}"
                traceback_message = traceback.format_exc()
                print(error_message)
                print(traceback_message)
                raise e

