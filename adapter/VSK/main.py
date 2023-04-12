import re
import os
import io
import sys
import boto3
import zipfile
import oci
import configparser
import pandas as pd
from minio import Minio
from datetime import datetime
from azure.storage.blob import BlobServiceClient

configuartion_path =os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

class CollectData:
    def __init__(self):
        '''
        Collecting all required Keys from config and from the arguments passing while running the script
        '''
        self.program      = sys.argv[1]
        self.input_file   = sys.argv[2]
        self.date_today = datetime.now().strftime('%d-%b-%Y')
        self.env          = config['CREDs']['storage_type']
        self.input_folder = 'emission/' + self.date_today+'/'+self.input_file
        self.output_folder = 'process_input/' + self.program + '/' + self.date_today

        if self.env == 'azure':
            # ______________AZURE Blob Config keys______________________
            self.azure_connection_string = config['CREDs']['azure_connection_string']
            self.azure_container         = config['CREDs']['azure_container']

            # ___________________AZURE Blob Connection___________________________
            try:
                self.blob_service_client = BlobServiceClient.from_connection_string(self.azure_connection_string)
                self.container_client = self.blob_service_client.get_container_client(self.azure_container)
            except Exception:
                print(f'Message : Failed to connect to azure {self.azure_container} container')

        elif self.env == 'aws':
            #__________________S3 Bucket Config Keys___________________
            self.aws_access_key     = config['CREDs']['aws_access_key']
            self.aws_secret_key     = config['CREDs']['aws_secret_key']
            self.s3_bucket          = config['CREDs']['s3_bucket']

            #__________________S3 Bucket Connection ____________________
            try:
                self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key,
                                       aws_secret_access_key=self.aws_secret_key)
                self.s3_objects_list = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.input_folder)
            except Exception:
                print(f'Message : Failed to connect to {self.s3_bucket} bucket')

        elif self.env == 'local':
            #___________________Minio Bucket Config Keys___________________

            self.minio_endpoint     = config['CREDs']['minio_end_point']
            self.minio_port         = config['CREDs']['minio_port']
            self.minio_username   = config['CREDs']['minio_username']
            self.minio_password  = config['CREDs']['minio_password']
            self.minio_bucket       = config['CREDs']['minio_bucket']

            #__________________ Minio Bucket Connection_________________
            try:
                self.minio_client = Minio(endpoint=self.minio_endpoint+':'+self.minio_port,access_key=self.minio_username,secret_key=self.minio_password,secure=False)  # set this to True if your Minio instance is secured with SSL/TLS
                self.minio_object_list=self.minio_client.list_objects(self.minio_bucket,prefix=self.input_folder, recursive=True)
            except Exception:
                print(f'Message : Failed to connect to {self.minio_bucket} bucket')

        elif self.env == 'oracle':
            #_________________ Oracle DB config Keys ____________________
            self.config = oci.config.from_file('~/.oci/config')
            self.oracle_storage_client = oci.object_storage.ObjectStorageClient(self.config)
            self.namespace = self.oracle_storage_client.get_namespace().data
            self.oracle_bucket=config['CREDs']['oracle_bucket']
            #_________________ Oracle DB connection _____________________
            try:
                self.file_contents = self.oracle_storage_client.get_object(self.namespace, self.oracle_bucket, self.input_folder)
            except oci.exceptions.ServiceError as e:
                if e.status == 404:
                    print(f"The object {self.input_folder} does not exist in the bucket {self.oracle_bucket}.")
                else:
                    raise e
        else:
            print(f'Message : Storage type {self.env} is not valid')

    #___________________________Column renaming after reading file from colud__________
        self.rep_list = []
    def column_rename(self,df):
        for col in df.columns.tolist():
            x=re.sub(r'^[\d.\s]+|[\d.\s]+$]+','',col)
            self.rep_list.append(x)
        col_list = df.columns.tolist()
        df_snap=df[col_list]
        df_snap.columns=self.rep_list
        return df_snap

    #__________________________Parsing the buffer data_________________________________

    def data_parser(self,data):
        with zipfile.ZipFile(data) as myzip:
            with myzip.open(myzip.namelist()[0]) as myfile:
                df = pd.read_csv(myfile)
                df_snap = self.column_rename(df)
                return df_snap

    #_____________________________Read the File from Cloud_____________________________

    def  get_file(self):

        if self.env == 'azure': ## reading frile from Azure cloud
            blobs_list = self.container_client.list_blobs(name_starts_with=self.input_folder)
            if any(blobs_list):
                blob_client = self.container_client.get_blob_client(blob=self.input_folder)
                data=io.BytesIO(blob_client.download_blob().readall())
                df_snap=self.data_parser(data)
                return df_snap
            else:
                print(f'Message : The folder {self.input_folder} not exists in azure blob container.')

        elif self.env == 'aws': ## Reading file from AWS cloud
            if 'Contents' in self.s3_objects_list:
                file_bytes =self.s3.get_object(Bucket=self.s3_bucket, Key=self.input_folder)
                data=io.BytesIO(file_bytes['Body'].read())
                df_snap=self.data_parser(data)
                return df_snap
            else:
                print(f"Message : The folder {self.input_folder} does not exist in the bucket {self.s3_bucket}.")

        elif self.env == 'local': ## Reading file from local Minio
            for obj in self.minio_object_list:
                if  obj.object_name:
                    object_data = self.minio_client.get_object(self.minio_bucket, self.input_folder)
                    data=io.BytesIO(object_data.read())
                    df_snap=self.data_parser(data)
                    return df_snap
                else:
                    return print(f'Message : Folder {self.input_folder} does not exist')
        elif self.env == 'oracle':
            list_objects_response = self.oracle_storage_client.list_objects(self.namespace, self.oracle_bucket)
            for object_summary in list_objects_response.data.objects:
                if object_summary.name == self.input_folder:
                    data = io.BytesIO(self.file_contents.data.content)
                    df_snap = self.data_parser(data)
                    return df_snap
        else:
            print(f'Message : Storage type {self.env} is not valid')

    #___________________________Upload the file to the cloud folder_______________________

    def upload_file(self,csv_data,output_file):
        csv_bytes = csv_data.to_csv(index=False).encode('utf-8')
        csv_buffer = io.BytesIO(csv_bytes)

        if self.env == 'azure': ## uploading file to Azure blob container
            blob_client=self.container_client.get_blob_client(blob=self.output_folder+'/'+output_file)
            blob_client.upload_blob(csv_buffer, overwrite=False)
            print(f"Message : File {output_file} uploaded successfully to the folder {self.output_folder}.")

        elif self.env == 'aws': ## uploading file to AWS bucket
            self.s3.put_object(Body=csv_buffer,Bucket=self.s3_bucket,Key=self.output_folder+'/'+output_file)
            print(f"Message : File {output_file} uploaded successfully to the folder {self.output_folder}.")

        elif self.env == 'local': ## uploading file to local Minio
            self.minio_client.put_object(bucket_name=self.minio_bucket,object_name=self.output_folder+'/'+output_file,data=csv_buffer,length=len(csv_bytes),content_type='application/csv')
            print(f"Message : File {output_file} uploaded successfully to the folder {self.output_folder}.")
        elif self.env == 'oracle':
            self.oracle_storage_client.put_object(self.namespace,self.oracle_bucket,self.output_folder,csv_buffer)
            print(f"Message : File {output_file} uploaded successfully to the folder {self.output_folder}.")
        else:
            print(f'Message : Storage type {self.env} is not valid')