import json
import glob
import os
import requests
import configparser
import pandas as pd
import time
configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/generators/transformers/python_files/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);
# Creating the class
class APIsIntegrator:
    def __init__(self):
        self.url_base = config['CREDs']['server_url']
        self.generator_host = config['CREDs']['generator_host']
        self.generator_port = config['CREDs']['generator_port']
        self.spec_host = config['CREDs']['spec_host']
        self.spec_port = config['CREDs']['spec_port']
        self.nifi_host = config['CREDs']['nifi_host']
        self.nifi_port = config['CREDs']['nifi_port']
        self.spec_url=self.spec_host+':'+self.spec_port
        self.generator_url = self.generator_host+':'+ self.generator_port
        self.nifi_url = self.nifi_host +':'+self.nifi_port
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.dataset_mapping = pd.read_csv(os.path.dirname(os.path.abspath(__file__)) + "/generators/key_files/transformer_dataset_mapping.csv")
        self.dimension_mapping = pd.read_csv(os.path.dirname(os.path.abspath(__file__)) + "/generators/key_files/transformer_dimension_mapping.csv")
        self.program = self.dataset_mapping['program'].drop_duplicates().tolist()
        self.keys_types=[['dataset_keys','DatasetSpec'],['event_keys','EventSpec'],['dimension_keys','DimensionSpec']]

    def generate_spec(self):
        url = self.generator_url + "/api/generator/spec"
        for program in self.program:
            for kt in self.keys_types:
                payload = json.dumps({
                    'key_file':kt[0]+'.csv',
                    'spec_type':kt[1],
                    'validation_keys':'additional_validation.csv',
                    'program':program
                })
                response = requests.request("POST", url, headers=self.headers, data=payload)
                re=response.json()
                print({"message": re['message'], "payload":payload })
                time.sleep(0.5)

    def insert_dimension_spec(self):
        for i in self.program:
            dimension_spec_files = glob.glob(os.path.dirname(os.path.abspath(__file__)) +'/generators/'+i+'_Specs/' + '*.json')
            url = self.spec_url + "/dimension"
            for file in dimension_spec_files:
                dimension = file.split('/')[-1].strip('.json')
                slice = dimension.split('_')[0]
                if slice == 'dimension':
                    with open(file, 'r') as f:
                        spec = json.load(f)
                    payload = json.dumps(spec)
                    response = requests.request("POST", url, headers=self.headers, data=payload)
                    re = response.json()
                    print({"message": re['message'], "Dimension": dimension})
                    time.sleep(0.5)

    def insert_event_spec(self):
        for i in self.program:
            event_spec_files = glob.glob(os.path.dirname(os.path.abspath(__file__)) +'/generators/'+i+'_Specs/' + '*.json')
            url = self.spec_url + "/event"
            for file in event_spec_files:
                event = file.split('/')[-1].strip('.json')
                slice = event.split('_')[0]
                if slice == 'event':
                    with open(file, 'r') as f:
                        spec = json.load(f)
                    payload = json.dumps(spec)
                    response = requests.request("POST", url, headers=self.headers, data=payload)
                    re = response.json()
                    print({"message": re['message'], "Event": event})
                    time.sleep(0.5)

    def insert_dataset_spec(self):
        for i in self.program:
            url = self.spec_url + "/dataset"
            dataset_spec_files = glob.glob(os.path.dirname(os.path.abspath(__file__)) +'/generators/'+i+'_Specs/' + '*.json')
            for file in dataset_spec_files:
                dataset = file.split('/')[-1].strip('.json')
                slice = dataset.split('_')[0]
                if slice not in ['event', 'dimension']:
                    with open(file, 'r') as f:
                        spec = json.load(f)
                        payload = json.dumps(spec)
                        response = requests.request("POST", url, headers=self.headers, data=payload)
                        re = response.json()
                        print({"message": re['message'], "Dataset": dataset})
                        time.sleep(0.5)

    def generate_dataset_transformers(self):
        url = self.spec_url + "/transformer"
        data_to_list = self.dataset_mapping[['program','event_name']].drop_duplicates().values.tolist()
        for file in data_to_list:
            payload = json.dumps({
                'ingestion_name': file[1],
                'key_file': 'transformer_dataset_mapping.csv',
                'program': file[0],
                'operation': 'dataset'
            })
            response = requests.request("POST", url, headers=self.headers, data=payload)
            re = response.json()
            print({"message": re['message'], "Transformer": payload})
            time.sleep(0.5)

    def generate_dimension_transformers(self):
        url = self.spec_url + "/transformer"
        data_to_list = self.dimension_mapping.values.tolist()
        for file in data_to_list:
             payload = json.dumps({
                          'ingestion_name': file[1],
                          'key_file': 'transformer_dimension_mapping.csv',
                          'program': "",
                          'operation': 'dimension'
                      })
             response = requests.request("POST", url, headers=self.headers, data=payload)
             re=response.json()
             print({"message": re['message'], "Transformer": payload})
             time.sleep(0.5)


    def create_pipeline_dataset(self):
        url = self.spec_url + "/pipeline"
        data_to_list = self.dataset_mapping.values.tolist()
        for file in data_to_list:
            payload = json.dumps({
                "pipeline_type": "ingest_to_db",
                "pipeline_name": file[3],
                "pipeline": [
                    {
                        "event_name": file[2],
                        "dataset_name": file[3],
                        "dimension_name": file[4],
                        "transformer_name": file[3] + '.py'
                    }
                ]
            })
            response = requests.request("POST", url, headers=self.headers, data=payload)
            re = response.json()
            print({"message": re['message'], "Pipeline": payload})
            time.sleep(0.5)

    def create_pipeline_dimension(self):
        url = self.spec_url + "/pipeline"
        data_to_list = self.dimension_mapping.values.tolist()
        for file in data_to_list:
            payload = json.dumps(
                {
                   "pipeline_type":"dimension_to_db",
                   "pipeline_name":file[1],
                   "pipeline": [
                    {
                      "dimension_name": file[1],
                      "transformer_name": file[1]+'.py'

                    }
                  ]
            })
            print("Payload of dimension pipeline is ::;;", payload)
            response = requests.request("POST", url, headers=self.headers, data=payload)
            re = response.json()
            print({"message": re['message'], "Pipeline": payload})
            time.sleep(0.5)
    def schedule_dimension(self):
        url=self.spec_url+'/schedule'
        dimension_schedule = self.dimension_mapping[['dimension_name','scheduler']].drop_duplicates().values.tolist()
        for ds in dimension_schedule:
            payload=json.dumps({
                 "pipeline_name":ds[0],
                 "scheduled_at": ds[1],
            })
            response = requests.request("POST", url, headers=self.headers, data=payload)
            re = response.json()
            print({"message": re['message'], "Scheduler": payload})
            time.sleep(0.5)
    def schedule_dataset(self):
        url=self.spec_url+'/schedule'
        dataset_schedule = self.dataset_mapping[['dataset_name','scheduler']].drop_duplicates().values.tolist()
        for ds in dataset_schedule:
            payload=json.dumps({
                 "pipeline_name":ds[0],
                 "scheduled_at": ds[1],
            })
            response = requests.request("POST", url, headers=self.headers, data=payload)
            re = response.json()
            print({"message": re['message'], "Scheduler": payload})
            time.sleep(0.5)
    def static_processor_group_creation(self):
        url = f'{self.generator_host}:{self.generator_port}/api/static_processor_group_creation'
        response = requests.request("POST", url, headers=self.headers)
        print(response)

# Creating the object of the class
obj = APIsIntegrator()

# Call the function using the object reference
obj.generate_spec()
obj.insert_dimension_spec()
obj.insert_event_spec()
obj.insert_dataset_spec()
obj.generate_dimension_transformers()
obj.generate_dataset_transformers()
obj.static_processor_group_creation()
obj.create_pipeline_dimension()
obj.create_pipeline_dataset()
obj.schedule_dimension()
obj.schedule_dataset()
