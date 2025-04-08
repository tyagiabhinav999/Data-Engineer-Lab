from boto3 import Session

class S3ClientProvider:
    def __init__(self, aws_access_key=None, aws_secret_key=None, region=None):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self.session = Session(
            aws_access_key_id = self.aws_access_key,
            aws_secret_access_key = self.aws_secret_key,
            region_name=self.region
        )
        self.s3_client = self.session.client('s3')

    def get_client(self):
        return self.s3_client
