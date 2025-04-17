import boto3
import pandas as pd
import io
import json
from datetime import datetime

# S3 bucket configuration
SOURCE_BUCKET = 'citibike-trips-data-1'
DESTINATION_BUCKET = 'citibike-trips-data-1'  # Can be same or different bucket
SCHEMA_KEY = 'schema/citibike_columns_schema.json'
PROCESSED_DATA_PREFIX = 'processed_data/'

def lambda_handler(event, context):
    """
    Lambda function to process Citibike trip data files, extract columns dynamically,
    and maintain a growing schema dictionary as new columns are discovered.
    
    This function expects to be triggered after unzip_citibike_files_lambda.py has run
    and placed CSV files in the result_files/ prefix.
    """
    s3_client = boto3.client('s3')
    
    # First, try to load existing schema from S3
    schema_dict = load_existing_schema(s3_client)
    
    # Get list of files to process from the event or scan the bucket
    # If this Lambda is triggered by S3 events when new files are added:
    if 'Records' in event and event['Records'][0]['eventSource'] == 'aws:s3':
        files_to_process = extract_files_from_event(event)
    elif 'files_to_process' in event:
        # If files are explicitly specified in the event
        files_to_process = event['files_to_process']
    else:
        # Otherwise, scan the bucket for files with the result_files/ prefix
        files_to_process = list_unprocessed_files(s3_client)
    
    processed_files = []
    for file_key in files_to_process:
        # Process each file
        print(f"Processing file: {file_key}")
        
        # Extract metadata from filename for organization
        file_metadata = extract_metadata_from_filename(file_key)
        
        # Process the file and update schema
        try:
            df = read_file_to_dataframe(s3_client, SOURCE_BUCKET, file_key)
            
            if df is not None and not df.empty:
                # Update the schema dictionary with new columns
                schema_dict = update_schema_with_new_columns(schema_dict, df, file_metadata)
                
                # Process and transform the data if needed (example: standardize column names)
                df = transform_data(df, schema_dict, file_metadata)
                
                # Save processed data to destination bucket with appropriate path
                destination_key = generate_destination_key(file_key, file_metadata)
                save_dataframe_to_s3(df, s3_client, DESTINATION_BUCKET, destination_key)
                
                processed_files.append({
                    "file": file_key,
                    "destination": destination_key,
                    "rows": len(df),
                    "columns": len(df.columns)
                })
            else:
                print(f"File {file_key} is empty or could not be read")
        except Exception as e:
            print(f"Error processing file {file_key}: {str(e)}")
    
    # After processing all files, save the updated schema back to S3
    save_schema_to_s3(schema_dict, s3_client)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processed {len(processed_files)} files',
            'processed_files': processed_files
        })
    }

def load_existing_schema(s3_client):
    """Load existing schema dictionary from S3 if it exists, otherwise return empty dict."""
    try:
        response = s3_client.get_object(Bucket=DESTINATION_BUCKET, Key=SCHEMA_KEY)
        schema_dict = json.loads(response['Body'].read().decode('utf-8'))
        return schema_dict
    except Exception as e:
        print(f"No existing schema found or error loading schema: {str(e)}")
        # Initialize with empty dictionary
        return {
            'columns': {},  # Maps column names to data types and metadata
            'column_mappings': {},  # Maps alternate column names to standardized names
            'sources': {},  # Tracks which columns appear in which data sources
            'last_updated': datetime.now().isoformat()
        }

def extract_files_from_event(event):
    """Extract file keys from S3 event notification."""
    files = []
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        # Only process files from the result_files/ prefix
        if key.startswith('result_files/') and key.lower().endswith(('.csv', '.csv.gz')):
            files.append(key)
    
    return files

def list_unprocessed_files(s3_client):
    """List all CSV files in the result_files/ prefix."""
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Get all objects with the result_files/ prefix
    page_iterator = paginator.paginate(
        Bucket=SOURCE_BUCKET,
        Prefix='result_files/'
    )
    
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.lower().endswith(('.csv', '.csv.gz')):
                    files.append(key)
    
    return files

def read_file_to_dataframe(s3_client, bucket, key):
    """Read a CSV file from S3 into a pandas DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Determine file type by extension
        file_ext = os.path.splitext(key.lower())[-1]
        
        if file_ext == '.csv' or key.lower().endswith('.csv.zip'):
            # Handle both uncompressed and gzipped CSV files
            if key.endswith('.gz'):
                df = pd.read_csv(io.BytesIO(response['Body'].read()), compression='gzip', 
                                low_memory=False, error_bad_lines=False, warn_bad_lines=True)
            else:
                df = pd.read_csv(io.BytesIO(response['Body'].read()), 
                                low_memory=False, error_bad_lines=False, warn_bad_lines=True)
        elif file_ext in ['.xlsx', '.xls']:
            # Handle Excel files
            import pandas as pd
            df = pd.read_excel(io.BytesIO(response['Body'].read()))
        else:
            print(f"Unsupported file type for {key}")
            return None
        
        # Basic data cleaning
        # 1. Strip whitespace from column names
        df.columns = df.columns.str.strip()
        
        return df
    except Exception as e:
        print(f"Error reading file {key}: {str(e)}")
        return None

def update_schema_with_new_columns(schema_dict, df, file_metadata):
    """Update schema dictionary with any new columns found in the dataframe."""
    year = file_metadata.get('year')
    month = file_metadata.get('month')
    region = file_metadata.get('region', 'nyc')
    
    if not year:
        year = str(datetime.now().year)
    
    # Create a unique key for this data source
    source_key = f"{region}_{year}"
    if month:
        source_key += f"_{month}"
    
    # Ensure source entry exists
    if 'sources' not in schema_dict:
        schema_dict['sources'] = {}
    
    if source_key not in schema_dict['sources']:
        schema_dict['sources'][source_key] = {
            'year': year,
            'month': month,
            'region': region,
            'columns': []
        }
    
    # Get column info
    for column in df.columns:
        column_name = column.strip().lower()  # Normalize column names
        data_type = str(df[column].dtype)
        
        # Record the column for this source
        if column_name not in schema_dict['sources'][source_key]['columns']:
            schema_dict['sources'][source_key]['columns'].append(column_name)
        
        # Add new column to the columns dictionary if it doesn't exist
        if column_name not in schema_dict['columns']:
            normalized_name = normalize_column_name(column_name, region)
            
            schema_dict['columns'][column_name] = {
                'data_type': data_type,
                'first_seen_year': year,
                'first_seen_month': month,
                'first_seen_region': region,
                'normalized_name': normalized_name
            }
            
            # Also add to column mappings for consistency
            if 'column_mappings' not in schema_dict:
                schema_dict['column_mappings'] = {}
                
            schema_dict['column_mappings'][column_name] = normalized_name
    
    # Update timestamp
    schema_dict['last_updated'] = datetime.now().isoformat()
    
    return schema_dict

def normalize_column_name(column_name, region='nyc'):
    """
    Create a standardized column name for consistent mapping.
    
    Examples:
    - 'start_station_id' and 'start station id' would both map to 'start_station_id'
    - 'tripduration' and 'trip_duration' would both map to 'trip_duration' 
    """
    # Replace spaces with underscores
    normalized = column_name.replace(' ', '_')
    
    # Handle variations based on region and known column mappings
    nyc_mapping = {
        'tripduration': 'trip_duration',
        'starttime': 'start_time',
        'stoptime': 'stop_time',
        'start_station_name': 'start_station_name',
        'end_station_name': 'end_station_name',
        'start_station_id': 'start_station_id',
        'end_station_id': 'end_station_id',
        'start_lat': 'start_latitude',
        'start_lng': 'start_longitude',
        'end_lat': 'end_latitude',
        'end_lng': 'end_longitude',
        'bikeid': 'bike_id',
        'usertype': 'user_type',
        'birth_year': 'birth_year',
        'gender': 'gender',
        'member_casual': 'member_type',
        'rideable_type': 'rideable_type',
        'started_at': 'start_time',
        'ended_at': 'stop_time',
        'start_station_latitude': 'start_latitude',
        'start_station_longitude': 'start_longitude',
        'end_station_latitude': 'end_latitude',
        'end_station_longitude': 'end_longitude',
        'start_lat': 'start_latitude',
        'start_lng': 'start_longitude',
        'end_lat': 'end_latitude',
        'end_lng': 'end_longitude',
        'station_id': 'start_station_id',
        'name': 'start_station_name'
    }
    
    jc_mapping = {
        # Jersey City specific mappings if different from NYC
        # Most should be the same but we can add here if needed
        'tripduration': 'trip_duration',
        'starttime': 'start_time',
        'stoptime': 'stop_time',
        'start_station_name': 'start_station_name',
        'end_station_name': 'end_station_name',
        'start_station_id': 'start_station_id',
        'end_station_id': 'end_station_id',
        'start_lat': 'start_latitude',
        'start_lng': 'start_longitude',
        'end_lat': 'end_latitude',
        'end_lng': 'end_longitude',
        'bikeid': 'bike_id',
        'usertype': 'user_type',
        'birth_year': 'birth_year',
        'gender': 'gender',
        'member_casual': 'member_type',
        'rideable_type': 'rideable_type',
        'started_at': 'start_time',
        'ended_at': 'stop_time',
        'start_station_latitude': 'start_latitude',
        'start_station_longitude': 'start_longitude',
        'end_station_latitude': 'end_latitude',
        'end_station_longitude': 'end_longitude',
        'start_lat': 'start_latitude',
        'start_lng': 'start_longitude',
        'end_lat': 'end_latitude',
        'end_lng': 'end_longitude',
        'station_id': 'start_station_id',
        'name': 'start_station_name'        
    }
    
    # Apply specific mappings based on region
    mapping = nyc_mapping if region == 'nyc' else jc_mapping
    
    # Apply specific mappings if they exist
    if normalized in mapping:
        return mapping[normalized]
    
    return normalized

def transform_data(df, schema_dict, file_metadata):
    """
    Apply transformations to the dataframe based on the schema.
    This standardizes column names using the normalized versions,
    and handles region-specific data transformations.
    """
    region = file_metadata.get('region', 'nyc')
    
    # Create a mapping of current column names to normalized column names
    column_mapping = {}
    for col in df.columns:
        col_lower = col.strip().lower()
        if col_lower in schema_dict.get('column_mappings', {}):
            column_mapping[col] = schema_dict['column_mappings'][col_lower]
        else:
            # If no mapping exists, use the normalized version of the current name
            column_mapping[col] = normalize_column_name(col_lower, region)
    
    # Rename the columns
    df = df.rename(columns=column_mapping)
    
    # Apply region-specific data transformations
    if region == 'jc':
        # Jersey City specific transformations
        
        # 1. Fix station IDs in older JC data (some used different formats)
        if 'start_station_id' in df.columns and df['start_station_id'].dtype == 'object':
            # Some JC data has station IDs with 'JC' prefix
            df['start_station_id'] = df['start_station_id'].astype(str).str.replace('JC-', '').str.replace('JC', '')
            # Try to convert to numeric but keep as string if it fails
            try:
                df['start_station_id'] = pd.to_numeric(df['start_station_id'])
            except:
                pass
                
        if 'end_station_id' in df.columns and df['end_station_id'].dtype == 'object':
            df['end_station_id'] = df['end_station_id'].astype(str).str.replace('JC-', '').str.replace('JC', '')
            try:
                df['end_station_id'] = pd.to_numeric(df['end_station_id'])
            except:
                pass
        
        # 2. Add region identifier to station names for clarity when combined with NYC data
        if 'start_station_name' in df.columns:
            # Only add prefix if it's not already there
            mask = ~df['start_station_name'].astype(str).str.startswith('JC - ')
            df.loc[mask, 'start_station_name'] = 'JC - ' + df.loc[mask, 'start_station_name'].astype(str)
            
        if 'end_station_name' in df.columns:
            mask = ~df['end_station_name'].astype(str).str.startswith('JC - ')
            df.loc[mask, 'end_station_name'] = 'JC - ' + df.loc[mask, 'end_station_name'].astype(str)
        
        # 3. Add region column for easier filtering
        df['region'] = 'jersey_city'
    else:
        # NYC specific transformations
        
        # Add region column for easier filtering
        df['region'] = 'new_york'
    
    # Handle data type conversions (applies to both regions)
    for col in df.columns:
        # Convert datetime columns
        if any(time_keyword in col.lower() for time_keyword in ['time', 'date', 'started_at', 'ended_at']):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass  # If conversion fails, keep as is
    
    # Add a source column to track data origin
    df['data_source'] = f"{region}_{file_metadata.get('year', 'unknown')}"
    if file_metadata.get('month'):
        df['data_source'] += f"_{file_metadata.get('month')}"
    
    return df

def save_dataframe_to_s3(df, s3_client, bucket, key):
    """Save a dataframe to S3 as a Parquet file for better performance with Glue."""
    # Convert to Parquet format for better compatibility with AWS Glue
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer)
    
    # Move buffer position to the beginning
    parquet_buffer.seek(0)
    
    # Ensure key ends with .parquet extension
    if not key.endswith('.parquet'):
        key = key.rsplit('.', 1)[0] + '.parquet'
    
    # Upload to S3
    s3_client.upload_fileobj(parquet_buffer, bucket, key)
    
    return key

def save_schema_to_s3(schema_dict, s3_client):
    """Save the updated schema dictionary back to S3."""
    schema_json = json.dumps(schema_dict, indent=2)
    s3_client.put_object(
        Body=schema_json,
        Bucket=DESTINATION_BUCKET,
        Key=SCHEMA_KEY,
        ContentType='application/json'
    )

def extract_metadata_from_filename(filename):
    """
    Extract metadata like year, month, region from a Citibike filename pattern.
    
    Examples:
    - 2021-citibike-tripdata.zip → {'year': '2021', 'region': 'nyc', 'type': 'annual'}
    - 202101-citibike-tripdata.csv.zip → {'year': '2021', 'month': '01', 'region': 'nyc', 'type': 'monthly'}
    - JC-202101-citibike-tripdata.csv.zip → {'year': '2021', 'month': '01', 'region': 'jc', 'type': 'monthly'}
    """
    import re
    
    metadata = {
        'year': None,
        'month': None,
        'region': 'nyc',  # Default to NYC
        'type': 'unknown'
    }
    
    base_filename = os.path.basename(filename)
    
    # Check if it's Jersey City data (JC prefix)
    if 'JC-' in base_filename:
        metadata['region'] = 'jc'
        base_part = base_filename.replace('JC-', '')
    else:
        base_part = base_filename
    
    # Try to extract year and month
    # Annual pattern: 2021-citibike-tripdata.zip
    annual_match = re.search(r'^(20\d{2})-citibike-tripdata', base_part)
    if annual_match:
        metadata['year'] = annual_match.group(1)
        metadata['type'] = 'annual'
        return metadata
    
    # Monthly pattern: 202101-citibike-tripdata.csv.zip
    monthly_match = re.search(r'^(20\d{2})(\d{2})-citibike-tripdata', base_part)
    if monthly_match:
        metadata['year'] = monthly_match.group(1)
        metadata['month'] = monthly_match.group(2)
        metadata['type'] = 'monthly'
        return metadata
    
    # If we can't parse the pattern but can find a year, use that
    year_match = re.search(r'(20\d{2})', base_part)
    if year_match:
        metadata['year'] = year_match.group(1)
        # Look for a month after identifying the year
        month_match = re.search(r'(20\d{2})(\d{2})', base_part)
        if month_match:
            metadata['month'] = month_match.group(2)
            metadata['type'] = 'monthly'
        else:
            metadata['type'] = 'annual'
    
    return metadata

def generate_destination_key(file_key, file_metadata):
    """Generate the destination key for processed data with appropriate organization."""
    year = file_metadata.get('year', 'unknown')
    month = file_metadata.get('month')
    region = file_metadata.get('region', 'nyc')
    
    # Base filename without path and extension
    filename = os.path.basename(file_key)
    base_name = filename.split('.')[0]
    
    # Build the destination path
    if month:
        # Monthly data: processed_data/nyc/2021/01/202101-citibike-tripdata.parquet
        # or: processed_data/jc/2021/01/JC-202101-citibike-tripdata.parquet
        destination_key = f"{PROCESSED_DATA_PREFIX}{region}/{year}/{month}/{base_name}.parquet"
    else:
        # Annual data: processed_data/nyc/2021/2021-citibike-tripdata.parquet
        destination_key = f"{PROCESSED_DATA_PREFIX}{region}/{year}/{base_name}.parquet"
    
    return destination_key