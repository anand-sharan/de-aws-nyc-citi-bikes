import boto3
import zipfile
from io import BytesIO

BUCKET = 'citibike-trips-data-1'
FILE_TO_UNZIP = '2023-citibike-tripdata.zip'

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    # Create the zip_obj variable which contains a reference to the file
    # in S3 that we will unzip.
    zip_obj = s3_resource.Object(bucket_name=BUCKET, key=FILE_TO_UNZIP)
    print (zip_obj)
    
    # Load a reference to the contents of the zipped file in S3 into
    # a ZipFile object built from a BytesIO buffer.
    buffer = BytesIO(zip_obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    
    # Iterate through each file inside the zipped file.
    # Unzip and store those files in the same bucket in results_files/
    for filename in z.namelist():
        file_info = z.getinfo(filename)
        s3_resource.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=BUCKET,
            Key='result_files/' + f'{filename}')
