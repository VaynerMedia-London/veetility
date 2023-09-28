class S3Bucket:
    def __init__(self, bucket_name, region_name, aws_access_key_id=None, aws_secret_access_key=None):
        """Initialize an S3 bucket object.
        
        Args:
            bucket_name (str): Name of the S3 bucket
            region_name (str): Name of the AWS region
            aws_access_key_id (str): AWS access key ID
            aws_secret_access_key (str): AWS secret access key
        
        Returns:
            S3Bucket: S3 bucket object"""
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_client = boto3.client(
            's3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)

    def upload_binary_file(self, local_file_path, s3_key):
        """Upload a binary file to an S3 bucket.
        
        Args:
            local_file_path (str): Path to the local file
            s3_key (str): S3 key for the uploaded file
        
        Returns:
            None"""
        try:
            with open(local_file_path, 'rb') as data:
                self.s3_client.upload_fileobj(data, self.bucket_name, s3_key)
            print(f"File '{local_file_path}' successfully uploaded to '{s3_key}' in bucket '{self.bucket_name}'.")
        except FileNotFoundError:
            print(f"File '{local_file_path}' not found.")
        except NoCredentialsError:
            print("AWS credentials not found.")
    
    def download_binary_file(self, s3_key, local_file_path):
        """Download a binary file from an S3 bucket.
        
        Args:
            s3_key (str): S3 key for the file to download
            local_file_path (str): Path to the local destination
        
        Returns:
            None"""
        try:
            with open(local_file_path, 'wb') as data:
                self.s3_client.download_fileobj(self.bucket_name, s3_key, data)
            print(f"File '{s3_key}' in bucket '{self.bucket_name}' successfully downloaded to '{local_file_path}'.")
        except FileNotFoundError:
            print(f"File '{local_file_path}' not found.")
        except NoCredentialsError:
            print("AWS credentials not found.")