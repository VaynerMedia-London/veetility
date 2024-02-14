import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime
from io import StringIO, BytesIO
import pandas as pd


class S3Bucket:
    """A class for interacting with AWS S3 buckets using the `boto3` library.

    This class provides methods to read from and write to S3 buckets, including uploading and downloading 
    files, listing bucket contents, and converting pandas DataFrames to CSV files for storage in S3. 

    Attributes:
        s3_client (boto3.client): A boto3 client for interacting with AWS S3.

    Methods:
        read_csv_to_df(bucket_name, file_name): Reads a CSV file from an S3 bucket and returns it as a DataFrame.
        list_s3_bucket_contents(bucket_name): Lists all objects in an S3 bucket.
        write_df_to_csv(bucket_name, df, filename): Writes a DataFrame to a CSV file and uploads it to an S3 bucket.
        save_df_to_s3_if_not_exists(bucket_name, df, df_name): Saves a DataFrame to S3 as a CSV file if it doesn't exist.
        upload_binary_file(bucket_name, local_file_path, s3_key): Uploads a binary file to an S3 bucket.
        download_binary_file(bucket_name, s3_key, local_file_path): Downloads a binary file from an S3 bucket.

    Example:
        >>> s3 = S3Bucket('my_aws_profile')
        >>> df = s3.read_csv_to_df('my_bucket', 'data.csv')
        >>> s3.write_df_to_csv('my_bucket', df, 'data_backup.csv')

    Note:
        Ensure that AWS credentials are properly configured for the `boto3` client. 
        The class requires an AWS profile name to be passed during initialization for setting up the `boto3` session.

    Raises:
        botocore.exceptions.ClientError: If operations on the S3 bucket fail due to client-side issues.
        botocore.exceptions.NoCredentialsError: If AWS credentials are not correctly configured or found."""
    
    def __init__(self, aws_profile):
        """Initializes the S3Bucket class with the specified AWS profile.

        This constructor sets up a new AWS session using the provided profile name and creates an S3 client for 
        interacting with AWS S3 services. The S3 client is stored as an attribute for use in other methods of 
        the class. This setup is essential for performing operations such as reading from and writing to S3 
        buckets.

        Args:
            aws_profile (str): The name of the AWS profile to use for creating the boto3 session. This profile 
                            should be configured in your AWS credentials file.

        Raises:
            boto3.exceptions.NoCredentialsError: If the specified AWS profile is not found or the credentials 
                                                are invalid.
            botocore.exceptions.ClientError: If the AWS session or S3 client creation fails due to other AWS client 
                                            side issues.

        Example:
            >>> s3 = S3Bucket('my_aws_profile')

        Note:
            Before using this class, ensure that the AWS credentials file is properly set up with the specified 
            profile and that the profile has the necessary permissions to access AWS S3 services."""

        session = boto3.Session(profile_name=aws_profile)
        self.s3_client = session.client("s3")

    def read_csv_to_df(self, bucket_name, folder_path='', file_name=''):
        """Reads a CSV file from a specified folder in an AWS S3 bucket and converts it into a pandas DataFrame.

        This function fetches the specified CSV file from the given folder in an S3 bucket and reads it 
        into a pandas DataFrame. The CSV file is read into memory as a binary buffer 
        using BytesIO, and then pandas is used to parse the CSV data from this buffer.

        Args:
            bucket_name (str): The name of the S3 bucket from which the CSV file is read.
            folder_path (str): The path of the folder within the S3 bucket. Defaults to the root of the bucket.
            file_name (str): The name (key) of the CSV file within the specified folder of the S3 bucket.

        Returns:
            pandas.DataFrame: A DataFrame containing the data from the CSV file.

        Raises:
            botocore.exceptions.ClientError: If the file is not found in the S3 bucket 
                                            or the request to S3 fails.
            ValueError: If the `bucket_name`, `folder_path`, or `file_name` is an empty string or None.

        Examples:
            >>> s3 = S3Bucket()
            >>> df = s3.read_csv_to_df('my_bucket', 'my_folder/', 'my_data.csv')
            >>> print(df)

        Note:
            Ensure that AWS credentials are properly configured and the `boto3` client 
            is initialized before calling this function. This function assumes that the 
            CSV file is properly formatted for use with pandas' `read_csv` function."""

        # Ensure the folder path ends with '/' if it's not empty and concatenate it with the file name
        full_file_path = (folder_path if folder_path.endswith('/') else folder_path + '/') + file_name if folder_path else file_name
        
        obj = self.s3_client.get_object(Bucket=bucket_name, Key=full_file_path)

        data_buffer = BytesIO(obj['Body'].read())

        return pd.read_csv(data_buffer)

    def list_s3_bucket_contents(self, bucket_name, folder_path=''):
        """Lists the contents of the specified folder in an S3 bucket.

        This method retrieves a list of all objects (files) in the given folder of an S3 bucket and returns their keys 
        (filenames). If the folder is empty, does not exist, or the specified bucket does not exist, it returns an empty list.

        Args:
            bucket_name (str): The name of the S3 bucket.
            folder_path (str): The path of the folder within the S3 bucket. Defaults to the root of the bucket.

        Returns:
            List[str]: A list of keys (filenames) of all objects in the specified folder of the S3 bucket. If the folder is 
                    empty, does not exist, or the bucket does not exist, an empty list is returned.

        Raises:
            boto3.exceptions.S3Error: If an error occurs while accessing the S3 bucket.

        Examples:
            >>> s3 = S3Bucket()
            >>> s3.list_s3_bucket_contents('my_bucket', 'my_folder/')
            ['my_folder/file1.csv', 'my_folder/file2.csv', 'my_folder/image1.png']

        Note:
            This method assumes that the AWS credentials and permissions are correctly set up to access the specified S3 bucket and folder."""

        # Ensure the folder path ends with '/' if it's not empty
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'

        # List objects in the specified folder of the bucket
        contents = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        
        # Extract the keys (filenames) of the objects in the folder
        return [item['Key'] for item in contents.get('Contents', [])]

    def write_df_to_file(self, bucket_name, df, filename, filetype='csv'):
        """Write a DataFrame to a file and upload it to an S3 bucket.

        Args:
            bucket_name (str): Name of the S3 bucket.
            df (pandas.DataFrame): DataFrame to be written to file.
            filename (str): The name of the file to be created.
            filetype (str, optional): Type of file to create ('csv' or 'json'). Defaults to 'csv'.

        Raises:
            ValueError: If the specified filetype is not supported.
        """
    
        if filetype == 'csv':
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            file_buffer = csv_buffer.getvalue()
            

        elif filetype == 'json':
            json_buffer = StringIO()
            df.to_json(json_buffer, orient='records', lines=True)
            file_buffer = json_buffer.getvalue()
            filename += ".json"

        else:
            raise ValueError("Unsupported filetype. Choose 'csv' or 'json'.")

        self.s3_client.put_object(Bucket=bucket_name, Key=filename, Body=file_buffer)
    
    def save_df_to_s3_if_not_exists(self, bucket_name, df, df_name, filetype, override=False):
        """Saves a pandas DataFrame to an S3 bucket as a CSV file if a file with the same name does not already exist.

        This method checks the specified S3 bucket for an existing file that matches the naming convention 
        '{df_name}_{current_date}.csv'. If such a file exists, it does not perform any action. Otherwise, it 
        saves the provided DataFrame to the S3 bucket with the constructed filename.

        Args:
            bucket_name (str): The name of the S3 bucket where the file will be saved.
            df (pandas.DataFrame): The DataFrame to be saved as a CSV file.
            df_name (str): The base name to be used for the CSV file. The current date in the format 'YYYY_MM_DD' 
                        will be appended to this base name.

        Returns:
            None: This method does not return anything. It either saves the file to S3 or prints a message indicating 
                that the file already exists.

        Raises:
            boto3.exceptions.S3UploadFailedError: If the upload to the S3 bucket fails.
            Exception: If any other error occurs during the process.

        Examples:
            >>> s3 = S3Bucket()
            >>> s3.save_df_to_s3_if_not_exists('my_bucket', my_dataframe, 'sales_data')
            File 'sales_data_2023_11_22.csv' successfully uploaded to bucket 'my_bucket'."""
        # Format today's date
        today_str = datetime.now().strftime("%Y_%m_%d")

        # Construct the filename with date
        filename = f"{df_name}_{today_str}"

        # Check if the file already exists in the S3 bucket
        existing_files = self.list_s3_bucket_contents(bucket_name)
        if override == False:
            if filename in existing_files:
                print(f"File '{filename}' already exists in the bucket '{bucket_name}'.")
                return

        # Convert DataFrame to CSV and upload to S3
        self.write_df_to_file(bucket_name, df, filename, filetype)
        print(f"File '{filename}' successfully uploaded to bucket '{bucket_name}'.")
    
    def download_s3_file(self, url, aws_access_key_id, aws_secret_access_key):
        ''' Download a file from an S3 Bucket given a url.
        Args:
            url: S3 URL
        Returns:
            File content as string
        '''

        # Extracting bucket and key from the provided URL
        parsed_url = url.split('/')
        bucket = parsed_url[3]
        key = "/".join(parsed_url[4:])

        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')

    def upload_binary_file(self, bucket_name, local_file_path, s3_key):
        """Upload a binary file to an S3 bucket.
        
        Args:
            local_file_path (str): Path to the local file
            s3_key (str): S3 key for the uploaded file
        
        Returns:
            None"""
        try:
            with open(local_file_path, 'rb') as data:
                self.s3_client.upload_fileobj(data, bucket_name, s3_key)
            print(f"File '{local_file_path}' successfully uploaded to '{s3_key}' in bucket '{bucket_name}'.")
        except FileNotFoundError:
            print(f"File '{local_file_path}' not found.")
        except NoCredentialsError:
            print("AWS credentials not found.")
    
    def download_binary_file(self, bucket_name, s3_key, local_file_path):
        """Download a binary file from an S3 bucket.
        
        Args:
            s3_key (str): S3 key for the file to download
            local_file_path (str): Path to the local destination
        
        Returns:
            None"""
        try:
            with open(local_file_path, 'wb') as data:
                self.s3_client.download_fileobj(bucket_name, s3_key, data)
            print(f"File '{s3_key}' in bucket '{bucket_name}' successfully downloaded to '{local_file_path}'.")
        except FileNotFoundError:
            print(f"File '{local_file_path}' not found.")
        except NoCredentialsError:
            print("AWS credentials not found.")