def generate_urls(years=None, months=None, regions=None):
    """
    Generate URLs for Citibike data based on years, months, and regions.
    
    Examples:
    - generate_urls(years=[2023]) -> All data for 2023 (yearly file)
    - generate_urls(years=[2023], months=[1,2,3]) -> Jan, Feb, Mar 2023 monthly files
    - generate_urls(years=[2023], regions=['JC']) -> Jersey City data for 2023
    """
    if not years:
        # Default to current year and previous year if not specified
        current_year = datetime.now().year
        years = [current_year, current_year - 1]
    
    if not regions:
        # Default to NYC (empty prefix) and Jersey City (JC-)
        regions = ['', 'JC-']
    
    urls = []
    
    # Generate URLs based on the parameters
    for year in years:
        # If months are specified, generate monthly URLs
        if months:
            for month in months:
                for region in regions:
                    # Format: <region>YYYYMM-citibike-tripdata.csv.zip or <region>YYYYMM-citibike-tripdata.zip
                    # Examples: 202301-citibike-tripdata.csv.zip or JC-202301-citibike-tripdata.csv.zip
                    filename = f"{region}{year}{month:02d}-citibike-tripdata"
                    
                    # Try both .csv.zip and .zip extensions
                    urls.append(f"{BASE_URL}{filename}.csv.zip")
                    urls.append(f"{BASE_URL}{filename}.zip")
        else:
            # Generate yearly file URLs if no months specified
            for region in regions:
                if region == '':  # NYC has yearly datasets
                    filename = f"{year}-citibike-tripdata.zip"
                    urls.append(f"{BASE_URL}{filename}")
    
    return urls

import boto3
import json
import urllib.request
import zipfile
from io import BytesIO
import time
import os

# Configuration
SOURCE_BUCKET = 'citibike-trips-data-1'
DESTINATION_BUCKET = 'citibike-trips-data-1'  # Can be same or different bucket
SCHEMA_KEY = 'schema/citibike_columns_schema.json'
PROCESSED_DATA_PREFIX = 'processed_data/'
RESULT_FILES_PREFIX = 'result_files/'
ARCHIVE_PREFIX = 'archive/'
BASE_URL = 'https://s3.amazonaws.com/tripdata/'

def lambda_handler(event, context):
    """
    Main orchestrator Lambda function that coordinates the Citibike ETL process:
    1. Downloads Citibike zip files based on provided URLs
    2. Unzips the files and stores them in S3
    3. Processes the CSV files, updates the schema, and stores as Parquet
    4. Triggers AWS Glue crawler to update the data catalog
    5. Performs cleanup by moving or deleting the original zip files
    """
    # Initialize clients
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    
    # Extract parameters from the event or use defaults
    urls = event.get('urls', [])
    filenames = event.get('filenames', [])
    years = event.get('years', [])
    months = event.get('months', [])
    regions = event.get('regions', [])  # NYC is default (empty), JC for Jersey City
    trigger_crawler = event.get('trigger_crawler', True)
    crawler_name = event.get('crawler_name', 'citibike-data-crawler')
    
    # Generate URLs based on years/months/regions if provided
    if not urls and (years or months):
        urls = generate_urls(years, months, regions)
    
    # Generate URLs based on specific filenames if provided
    if not urls and filenames:
        urls = [f"{BASE_URL}{filename}" for filename in filenames]
    
    if not urls:
        # If no URLs are provided, check if we're processing existing files
        process_existing = event.get('process_existing', False)
        if process_existing:
            return process_existing_files(s3_client, glue_client, crawler_name, trigger_crawler)
        else:
            return {
                'statusCode': 400,
                'body': json.dumps('No Citibike data URLs provided and process_existing is false')
            }
    
    results = []
    for url in urls:
        try:
            # Step 1: Download file
            file_result = download_citibike_file(url)
            
            # Step 2: Unzip file
            if file_result['success']:
                unzip_result = unzip_citibike_file(file_result['zip_key'])
                
                # Step 3: Process the CSV files
                if unzip_result['success']:
                    process_result = process_citibike_files(unzip_result['csv_keys'])
                    
                    # Step 4: Clean up by moving the zip file to archive
                    cleanup_result = cleanup_files(file_result['zip_key'])
                    
                    results.append({
                        'url': url,
                        'download': file_result,
                        'unzip': unzip_result,
                        'process': process_result,
                        'cleanup': cleanup_result
                    })
                else:
                    results.append({
                        'url': url,
                        'download': file_result,
                        'unzip': unzip_result,
                        'error': 'Failed to unzip file'
                    })
            else:
                results.append({
                    'url': url,
                    'download': file_result,
                    'error': 'Failed to download file'
                })
        except Exception as e:
            results.append({
                'url': url,
                'error': str(e)
            })
    
    # Step 4: Trigger Glue crawler if requested
    crawler_status = None
    if trigger_crawler:
        crawler_status = trigger_glue_crawler(glue_client, crawler_name)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processed {len(urls)} Citibike data URLs',
            'results': results,
            'crawler': crawler_status
        })
    }

def download_citibike_file(url):
    """Download a Citibike zip file from a URL and upload to S3."""
    s3_client = boto3.client('s3')
    
    # Extract filename from URL
    filename = url.split('/')[-1]
    local_path = f"/tmp/{filename}"
    
    try:
        # Download file
        urllib.request.urlretrieve(url, local_path)
        
        # Upload to S3
        s3_key = filename
        s3_client.upload_file(local_path, SOURCE_BUCKET, s3_key)
        
        # Remove local file to free up space
        os.remove(local_path)
        
        return {
            'success': True,
            'zip_key': s3_key,
            'message': f'Successfully downloaded and uploaded {filename}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Failed to download or upload {filename}'
        }

def unzip_citibike_file(zip_key):
    """Unzip a Citibike zip file in S3 and store the contents."""
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    try:
        # Get the zip file from S3
        zip_obj = s3_resource.Object(bucket_name=SOURCE_BUCKET, key=zip_key)
        buffer = BytesIO(zip_obj.get()["Body"].read())
        
        # Create a zipfile object
        z = zipfile.ZipFile(buffer)
        
        csv_keys = []
        
        # Extract each file
        for filename in z.namelist():
            # Only process CSV files
            # Citibike files could be .csv or other extensions like .xlsx
            if filename.lower().endswith(('.csv', '.xlsx', '.xls')):
                # Create destination key in the result_files/ prefix
                # Include the original zip name in the path to prevent naming conflicts
                zip_basename = os.path.basename(zip_key).replace('.zip', '')
                destination_key = f"{RESULT_FILES_PREFIX}{zip_basename}/{filename}"
                
                # Upload the file to S3
                s3_resource.meta.client.upload_fileobj(
                    z.open(filename),
                    Bucket=SOURCE_BUCKET,
                    Key=destination_key
                )
                
                csv_keys.append(destination_key)
        
        return {
            'success': True,
            'csv_keys': csv_keys,
            'message': f'Successfully extracted {len(csv_keys)} files from {zip_key}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Failed to unzip {zip_key}'
        }

def process_citibike_files(csv_keys):
    """Process the extracted CSV files to update the schema and convert to Parquet."""
    # This function creates an event to pass to the process_citibike_data_lambda function
    # We'll simulate calling it directly here
    
    # Import the process_citibike_data_lambda module's handler function
    # In a real Lambda environment, you'd use AWS Lambda SDK to invoke another function
    try:
        import process_citibike_data_lambda
        
        # Create an event with the files to process
        event = {
            'files_to_process': csv_keys
        }
        
        # Process the files
        result = process_citibike_data_lambda.lambda_handler(event, None)
        
        return {
            'success': True,
            'processed_files': json.loads(result['body'])['processed_files'],
            'message': json.loads(result['body'])['message']
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to process CSV files'
        }

def process_existing_files(s3_client, glue_client, crawler_name, trigger_crawler):
    """Process existing files in the result_files/ prefix without downloading new data."""
    try:
        # List all CSV files in the result_files/ prefix
        files_to_process = []
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=SOURCE_BUCKET,
            Prefix=RESULT_FILES_PREFIX
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.lower().endswith(('.csv', '.csv.gz')):
                        files_to_process.append(key)
        
        if not files_to_process:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No existing CSV files found to process',
                    'files': []
                })
            }
        
        # Process the files
        import process_citibike_data_lambda
        
        event = {
            'files_to_process': files_to_process
        }
        
        result = process_citibike_data_lambda.lambda_handler(event, None)
        
        # Trigger Glue crawler if requested
        crawler_status = None
        if trigger_crawler:
            crawler_status = trigger_glue_crawler(glue_client, crawler_name)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Processed {len(files_to_process)} existing files',
                'processed_files': json.loads(result['body'])['processed_files'],
                'crawler': crawler_status
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process existing files',
                'error': str(e)
            })
        }

def cleanup_files(zip_key):
    """Clean up by moving processed zip files to an archive location or deleting them."""
    s3_client = boto3.client('s3')
    
    try:
        # Option 1: Move to archive folder
        archive_key = f"archive/{zip_key}"
        
        # Copy to archive location
        s3_client.copy_object(
            Bucket=SOURCE_BUCKET,
            CopySource={'Bucket': SOURCE_BUCKET, 'Key': zip_key},
            Key=archive_key
        )
        
        # Delete original
        s3_client.delete_object(
            Bucket=SOURCE_BUCKET,
            Key=zip_key
        )
        
        return {
            'success': True,
            'message': f'Successfully moved {zip_key} to archive location',
            'archive_location': archive_key
        }
        
        # Option 2: Delete directly (uncomment to use this option instead)
        # s3_client.delete_object(
        #     Bucket=SOURCE_BUCKET,
        #     Key=zip_key
        # )
        # 
        # return {
        #     'success': True,
        #     'message': f'Successfully deleted {zip_key}'
        # }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Failed to clean up {zip_key}'
        }

def trigger_glue_crawler(glue_client, crawler_name):
    """Trigger an AWS Glue crawler to update the data catalog."""
    try:
        # Check if crawler is ready to run
        response = glue_client.get_crawler(Name=crawler_name)
        
        if response['Crawler']['State'] == 'READY':
            # Start the crawler
            glue_client.start_crawler(Name=crawler_name)
            return {
                'success': True,
                'message': f'Successfully triggered crawler {crawler_name}'
            }
        else:
            return {
                'success': False,
                'message': f'Crawler {crawler_name} is not in READY state. Current state: {response["Crawler"]["State"]}'
            }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Failed to trigger crawler {crawler_name}'
        }