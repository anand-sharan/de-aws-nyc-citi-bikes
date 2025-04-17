import urllib.request
import boto3
from botocore.exceptions import ClientError

REMOTE_URL = 'https://s3.amazonaws.com/tripdata/2021-citibike-tripdata.zip'
LOCAL_FILE_NAME = '2021-citibike-tripdata.zip'
BUCKET_NAME = 'citibike-trips-data-1'

# only /tmp/ is writable in Lambda
LOCAL_FILE_PATH = '/tmp/' + LOCAL_FILE_NAME


def lambda_handler(event, context):
    urllib.request.urlretrieve(REMOTE_URL, LOCAL_FILE_PATH)
    
    file_name = LOCAL_FILE_PATH    # the name of the input file
    object_name = LOCAL_FILE_NAME  # the name of the object in S3
    bucket = BUCKET_NAME           # the bucket you want to upload the file to
    
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
