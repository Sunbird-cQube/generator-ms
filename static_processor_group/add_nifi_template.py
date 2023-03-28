import configparser
import random

import requests
import os


configuartion_path = os.path.dirname(os.path.abspath(__file__))+"/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

nifi_host = config['CREDs']['nifi_host']
nifi_port = config['CREDs']['nifi_port']
processing_time = config['CREDs']['processing_time']
plugin_time = config['CREDs']['plugin_time']
header = {"Content-Type": "application/json"}

def get_nifi_root_pg():
    """ Fetch nifi root processor group ID"""
    res = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/process-groups/root',  verify=True)
    if res.status_code == 200:
        global nifi_root_pg_id
        nifi_root_pg_id = res.json()['component']['id']
        # print(nifi_root_pg_id)
        return res.json()['component']['id']
    else:
        return res.text

def get_processor_group_info(processor_group_name):
    """
    Get procesor group details
    """
    nifi_root_pg_id = get_nifi_root_pg()
    pg_list = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{nifi_root_pg_id}')
    if pg_list.status_code == 200:
        # Iterate over processGroups and find the required processor group details
        for i in pg_list.json()['processGroupFlow']['flow']['processGroups']:
            if i['component']['name'] == processor_group_name:
                global processor_group
                processor_group = i
                return i
    else:
        return 'failed to list the processor groups'

def get_processor_group_ports(processor_group_name):
    # Get processor group details
    global pg_source
    pg_source = get_processor_group_info(processor_group_name)
    pg_details = requests.get(f"{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{pg_source['component']['id']}")
    if pg_details.status_code != 200:
        return pg_details.text
    else:
        return pg_details
def get_processor_group_id(processor_group_name):
    nifi_root_pg_id = get_nifi_root_pg()
    pg_list = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{nifi_root_pg_id}')
    if pg_list.status_code == 200:
        # Iterate over processGroups and find the required processor group details
        for i in pg_list.json()['processGroupFlow']['flow']['processGroups']:
            if i['component']['name'] == processor_group_name:
                id = i['component']['id']
                return id


def upload_template(template_files):
    root_pg_id = get_nifi_root_pg()
    payload = {'template': open(template_files, 'rb')}
    get_template_upload = requests.post(f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_pg_id}/templates/upload", files=payload)
    if get_template_upload.ok:
        print('Successfully uploaded the template',template_files)
    else:
        print("Failed to upload the template ",get_template_upload.text)

def get_template_id(processor_group):
    '''Get template id '''
    get_template = requests.get(f"{nifi_host}:{nifi_port}/nifi-api/flow/templates")
    data = get_template.json()
    for template in data['templates']:
        if template['template']['name'] == processor_group:
            template_id = template['template']['id']
            return template_id

def instantiate_template_codes(processor_group):
    # Instantiates template
    root_pg_id = get_nifi_root_pg()
    data = {}
    if processor_group.__contains__('aws'):
        template_id = get_template_id('Run Latest Code aws')
        data = {
            "templateId": template_id,
            "originX": -1120,
            "originY": -1080,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('local'):
        template_id = get_template_id('Run Latest Code local')
        data = {
            "templateId": template_id,
            "originX": -1120,
            "originY": -1080,
            "disconnectedNodeAcknowledged": "false"
        }
    get_import_template = requests.post(
        f'{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_pg_id}/template-instance', json=data)
    if get_import_template.ok:
        print(f"Successfully instantiated the {processor_group} in nifi canvas")
    else:
        print(f"Failed to instantiate the {processor_group} in nifi canvas ", get_import_template.text)



def instantiate_template(processor_group):
    # Instantiates template
    root_pg_id = get_nifi_root_pg()
    data = {}
    if processor_group.__contains__('adaptors'):
        template_id = get_template_id('Run_adaptors')
        data = {
            "templateId": template_id,
            "originX": -1067.5854405025766,
            "originY": -1529.7644241816233,
            "disconnectedNodeAcknowledged": "false"
        }
    if processor_group.__contains__('Code'):
        template_id = get_template_id('Run Latest Code')
        data = {
        "templateId": template_id,
        "originX": -1067.5854405025766,
        "originY": -1529.7644241816233,
        "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('Student'):
        template_id = get_template_id('Plugin Student Attendance aws')
        data = {
            "templateId": template_id,
            "originX": -1200,
            "originY": -1600,
            "disconnectedNodeAcknowledged": "false"
        }

    elif processor_group.__contains__('Teachers'):
        template_id = get_template_id('Plugin Teachers Attendance aws')
        data = {
            "templateId": template_id,
            "originX": -1200,
            "originY": -1600,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('Rev-and-monitor'):
        template_id = get_template_id('Plugin Rev-and-monitor aws')
        data = {
            "templateId": template_id,
            "originX": -1250,
            "originY": -1550,
            "disconnectedNodeAcknowledged": "false"
        }
    get_import_template = requests.post(f'{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_pg_id}/template-instance', json=data)
    if get_import_template.ok:
        print(f"Successfully instantiated the {processor_group} in nifi canvas")
    else:
        print(f"Failed to instantiate the {processor_group} in nifi canvas ", get_import_template.text)

def instantiate_template_local(processor_group):
    # Instantiates template
    root_pg_id = get_nifi_root_pg()
    data = {}
    if processor_group.__contains__('Student'):
        template_id = get_template_id('Plugin Student Attendance local')
        data = {
            "templateId": template_id,
            "originX": -1200,
            "originY": -1600,
            "disconnectedNodeAcknowledged": "false"
        }

    elif processor_group.__contains__('Teachers'):
        template_id = get_template_id('Plugin Teachers Attendance local')
        data = {
            "templateId": template_id,
            "originX": -1200,
            "originY": -1600,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('Rev-and-monitor'):
        template_id = get_template_id('Plugin Rev-and-monitor local')
        data = {
            "templateId": template_id,
            "originX": -1250,
            "originY": -1550,
            "disconnectedNodeAcknowledged": "false"
        }
    get_import_template = requests.post(f'{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_pg_id}/template-instance', json=data)
    if get_import_template.ok:
        print(f"Successfully instantiated the {processor_group} in nifi canvas")
    else:
        print(f"Failed to instantiate the {processor_group} in nifi canvas ", get_import_template.text)

def get_controller_services_id(processor_group_name,controllers):
    processor_group_id = get_processor_group_id(processor_group_name)
    list_controllers = requests.get(f"{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{processor_group_id}/controller-services")
    controllers_list = list_controllers.json()
    for i in controllers_list['controllerServices']:
        if i['component']['name'] == controllers:
            controllers_list_id = i['component']['id']
            return controllers_list_id

def get_controller_services_details(processor_group_name):
    processor_group_id = get_processor_group_id(processor_group_name)
    list_controllers = requests.get(
        f"{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{processor_group_id}/controller-services")
    controllers_list = list_controllers.json()
    return controllers_list

def update_controller_service_property(processor_group_name, controller_name):
    controller_details = get_controller_services_details(processor_group_name)
    for i in controller_details['controllerServices']:
        if i['component']['name'] == controller_name:
            # Request body for aws controller
            update_controller_body_aws = {"revision": {
                "version": i['revision']['version'],
                "lastModifier": "Python"
            },
                "component": {
                    "id": i['component']['id'],
                    "name": controller_name,
                    "properties":
                        {
                            "Access Key": config['CREDs']['s3_access_key'],
                            "Secret Key": config['CREDs']['s3_secret_key']
                        }
                }

            }
            # controller body selection based on controller name
            update_controller_body = update_controller_body_aws

            update_controller_res = requests.put(
                f"{nifi_host}:{nifi_port}/nifi-api/controller-services/{i['component']['id']}",
                json=update_controller_body, headers=header)
            if update_controller_res.status_code == 200:
                return True
            else:
                return update_controller_res.text

# Enabling the controller service
def controller_service_enable(processor_group_name):
    controller_details = get_controller_services_details(processor_group_name)
    for i in controller_details['controllerServices']:
        if i['component']['state'] == 'DISABLED':

            controller_service_enable_body = {"revision": {
                "version": i['revision']['version'], }, "state": "ENABLED"}
            controller_service_enable_res = requests.put(f"{nifi_host}:{nifi_port}/nifi-api/controller-services/{i['component']['id']}/run-status",
                                                   json=controller_service_enable_body, headers=header)
            if controller_service_enable_res.status_code == 200:
                print("Successfully enabled the controll services in ", processor_group_name)
            else:
                print("Failed to enabled in ",processor_group_name)

# Disabling the controller service
def controller_service_disable(processor_group_name):
    controller_details = get_controller_services_details(processor_group_name)
    for i in controller_details['controllerServices']:
        if i['component']['state'] == 'ENABLED':

            controller_service_enable_body = {"revision": {
                "version": i['revision']['version'], }, "state": "DISABLED"}
            controller_service_enable_res = requests.put(f"{nifi_host}:{nifi_port}/nifi-api/controller-services/{i['component']['id']}/run-status",
                                                   json=controller_service_enable_body, headers=header)
            if controller_service_enable_res.status_code == 200:
                print("Successfully enabled the controll services in ", processor_group_name)
            else:
                print("Failed to enabled in ",processor_group_name)

#Updating the processor property
def update_processor_property(processor_group_name, processor_name):
    # Get the processors in the processor group
    pg_source = get_processor_group_ports(processor_group_name)
    if pg_source.status_code == 200:
        for i in pg_source.json()['processGroupFlow']['flow']['processors']:
            # Get the required processor details
            if i['component']['name'] == processor_name:
                # Request body creation to update processor property.
                global update_processor_property_body
                if processor_name == 'GenerateFlowFile_adapter':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "schedulingPeriod": config['CREDs']['adaptor_schedule_time'],
                                "schedulingStrategy": "CRON_DRIVEN"
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }

                if processor_name == 'FetchS3Object_aws':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "properties": {
                                    "Bucket": config['CREDs']['s3_input_bucket'],
                                    "Access Key": config['CREDs']['s3_access_key'],
                                    "Secret Key": config['CREDs']['s3_secret_key']
                                },
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }

                elif processor_name == 'FetchS3Object' or processor_name == 'FetchS3Object' or  processor_name == 'Puts3Processing1'or  processor_name == 'Puts3Processing2'or  processor_name == 'Puts3Processing3':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "properties": {
                                    "Bucket": config['CREDs']['s3_input_bucket']
                                },
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'GenerateFlowFile':
                    update_processor_property_body ={
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "schedulingPeriod": processing_time,
                                "schedulingStrategy": "CRON_DRIVEN"
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'Lists3':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "schedulingPeriod": processing_time,
                                "schedulingStrategy": "CRON_DRIVEN",
                                "properties": {
                                    "Bucket": config['CREDs']['s3_input_bucket']
                                },
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'ListS3Files':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "schedulingPeriod": processing_time,
                                "schedulingStrategy": "CRON_DRIVEN",
                                "properties": {
                                    "Bucket": config['CREDs']['s3_input_bucket'],
                                    "Access Key": config['CREDs']['s3_access_key'],
                                    "Secret Key": config['CREDs']['s3_secret_key'],
                                    "prefix": "process_input/${now():format('dd-MMM-yyyy')}/"

                                },
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }


                elif processor_name == 'Lists3_local':
                    endpoint_url = config['CREDs']['minio_end_point']
                    port = config['CREDs']['minio_port']
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "schedulingPeriod": plugin_time,
                                "schedulingStrategy": "CRON_DRIVEN",
                                "properties": {
                                    "Bucket": config['CREDs']['minio_bucket'],
                                    "Access Key": config['CREDs']['minio_access_key'],
                                    "Secret Key": config['CREDs']['minio_secret_key'],
                                    "Endpoint Override URL": f"{endpoint_url}:{port}"
                                },
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }
                elif processor_name == 'ListLocal':
                    endpoint_url = config['CREDs']['minio_end_point']
                    port = config['CREDs']['minio_port']
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "schedulingPeriod": plugin_time,
                                "schedulingStrategy": "CRON_DRIVEN",
                                "properties": {
                                    "Bucket": config['CREDs']['minio_bucket'],
                                    "Access Key": config['CREDs']['minio_access_key'],
                                    "Secret Key": config['CREDs']['minio_secret_key'],
                                    "Endpoint Override URL": f"{endpoint_url}:{port}",
                                    "prefix": "process_input/${now():format('dd-MMM-yyyy')}/"
                                },
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }

                elif processor_name == 'FetchS3Object_local' or processor_name == 'FetchS3Object_local' or  processor_name == 'Puts3Processing1_local'or  processor_name == 'Puts3Processing2_local'or  processor_name == 'Puts3Processing3_local':
                    endpoint_url = config['CREDs']['minio_end_point']
                    port = config['CREDs']['minio_port']
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "properties": {
                                    "Bucket": config['CREDs']['minio_bucket'],
                                    "Access Key": config['CREDs']['minio_access_key'],
                                    "Secret Key": config['CREDs']['minio_secret_key'],
                                    "Endpoint Override URL": f"{endpoint_url}:{port}"
                                },
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": "false"
                    }

                # API call to update the processor property
                update_processor_res = requests.put(
                    f"{nifi_host}:{nifi_port}/nifi-api/processors/{i['component']['id']}",
                    json=update_processor_property_body)
                if update_processor_res.status_code == 200:
                    print(f"Successfully updated the properties in the {processor_name}")
                    return True

                else:
                    return update_processor_res.text

def start_processor_group(processor_group_name,state):
    header = {"Content-Type": "application/json"}
    pg_source = get_processor_group_info(processor_group_name)
    start_body = {
        "id": pg_source['component']['id'],
        "state": state,
        "disconnectedNodeAcknowledged": False}
    start_response = requests.put(f"{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{pg_source['component']['id']}",
                            json=start_body, headers=header)
    if start_response.status_code == 200:
        print(f"Successfully {state} {pg_source['component']['name']} Processor Group.")
        return True
    else:
        return start_response.text

def plugins_aws():
    upload_template('Plugin_Rev-and-monitor_aws.xml')
    instantiate_template('Plugin_Rev-and-monitor_aws.xml')
    controller_service_disable('Plugin Rev-and-monitor aws')
    update_controller_service_property('Plugin Rev-and-monitor aws', 'aws_rev-mon')
    controller_service_enable('Plugin Rev-and-monitor aws')

    upload_template('Plugin_Student_Attendance_aws.xml')
    instantiate_template('Plugin_Student_Attendance_aws.xml')
    controller_service_disable('Plugin Student Attendance aws')
    update_controller_service_property('Plugin Student Attendance aws', 'aws_credentials_students')
    controller_service_enable('Plugin Student Attendance aws')

    upload_template('Plugin_Teachers_Attendance_aws.xml')
    instantiate_template('Plugin_Teachers_Attendance_aws.xml')
    controller_service_disable('Plugin Teachers Attendance aws')
    update_controller_service_property('Plugin Teachers Attendance aws', 'aws_credentials_teachers')
    controller_service_enable('Plugin Teachers Attendance aws')

    processors = ['Lists3', 'FetchS3Object', 'Puts3Processing1', 'Puts3Processing2', 'Puts3Processing3']
    for i in processors:
        update_processor_property('Plugin Student Attendance aws', i)
        update_processor_property('Plugin Teachers Attendance aws', i)
    update_processor_property('Plugin Rev-and-monitor aws', 'Lists3')
    update_processor_property('Plugin Rev-and-monitor aws', 'Puts3Processing')

    processor_list = ['Plugin Student Attendance aws','Plugin Student Attendance aws','Plugin Rev-and-monitor aws']
    for i in processor_list:
        start_processor_group(i, 'RUNNING')


def plugins_local():
    upload_template('Plugin_Rev-and-monitor_local.xml')
    instantiate_template_local('Plugin_Rev-and-monitor_local.xml')

    upload_template('Plugin_Student_Attendance_local.xml')
    instantiate_template_local('Plugin_Student_Attendance_local.xml')
    controller_service_disable('Plugin Student Attendance local')
    controller_service_enable('Plugin Student Attendance local')

    upload_template('Plugin_Teachers_Attendance_local.xml')
    instantiate_template_local('Plugin_Teachers_Attendance_local.xml')
    controller_service_disable('Plugin Teachers Attendance local')
    controller_service_enable('Plugin Teachers Attendance local')

    processors = ['Lists3_local', 'FetchS3Object_local', 'Puts3Processing1_local', 'Puts3Processing2_local', 'Puts3Processing3_local']
    for i in processors:
        update_processor_property('Plugin Student Attendance local', i)
        update_processor_property('Plugin Teachers Attendance local', i)
    update_processor_property('Plugin Rev-and-monitor local', 'Lists3_local')
    update_processor_property('Plugin Rev-and-monitor local', 'Puts3Processing_local')
    processor_group_list = ['Plugin Teachers Attendance local', 'Plugin Student Attendance local',
                            'Plugin Rev-and-monitor local']
    for i in processor_group_list:
        update_processor_property(i, 'RUNNING')

def run_latest_aws():
    upload_template('Run_Latest_Code_aws.xml')
    instantiate_template_codes('Run_Latest_Code_aws.xml')
    update_processor_property('Run Latest Code aws', 'ListS3Files')
    update_processor_property('Run Latest Code aws', 'FetchS3Object_aws')
    start_processor_group('Run Latest Code aws', 'RUNNING')

def run_latest_local():
    upload_template('Run_Latest_Code_local.xml')
    instantiate_template_codes('Run_Latest_Code_local.xml')
    update_processor_property('Run Latest Code local', 'ListLocal')
    update_processor_property('Run Latest Code local', 'FetchS3Object_local')
    start_processor_group('Run Latest Code local', 'RUNNING')



if __name__ == '__main__':
    if config['CREDs']['storage_type'] == 'aws':
        plugins_aws()
        run_latest_aws()

    if config['CREDs']['storage_type'] == 'local':
        plugins_local()
        run_latest_local()
    upload_template('Run_adaptors.xml')
    instantiate_template('Run_adaptors.xml')
    update_processor_property('Run_adaptors','GenerateFlowFile_adapter')