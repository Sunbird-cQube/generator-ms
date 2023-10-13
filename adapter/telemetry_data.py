from datetime import datetime
import boto3
import os
import configparser

configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/VSK/config.ini"
print(configuartion_path)
config = configparser.ConfigParser()
config.read(configuartion_path);

if config['CREDs']['storage_type'] == 'aws':
    os.environ['AWS_ACCESS_KEY_ID'] = config['CREDs']['aws_access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['CREDs']['aws_secret_key']
    date_today = datetime.now().strftime('%d-%b-%Y')
    input_folder = 'process_input/telemetry/'+date_today
    bucket_name = config['CREDs']['s3_bucket']
    local_base_path = '.'
    s3 = boto3.client('s3')

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_folder)
    # Loop through the objects and download them
        for obj in response.get('Contents', []):
            # print(obj)
            file_key = obj['Key']
            # Create local directory structure based on S3 path
            local_path = os.path.join(local_base_path, os.path.dirname(file_key))
            if not os.path.exists(local_path):
                os.makedirs(local_path)
            # Download and save the file locally
            local_file_path = os.path.join(local_path, os.path.basename(file_key))
            s3.download_file(bucket_name, file_key, local_file_path)
            print(f"Downloaded file: {file_key} to {local_file_path}")
    except Exception as e:
        print(f"Error: {e}")
