import configparser
import requests
import os

configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

nifi_host = config['CREDs']['nifi_host']
nifi_port = config['CREDs']['nifi_port']
processing_time = config['CREDs']['processing_time']
plugin_time = config['CREDs']['plugin_time']
header = {"Content-Type": "application/json"}


def get_nifi_root_pg():
    """ Fetch nifi root processor group ID"""
    res = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/process-groups/root', verify=True)
    if res.status_code == 200:
        global nifi_root_pg_id
        nifi_root_pg_id = res.json()['component']['id']
        # print(nifi_root_pg_id)
        return res.json()['component']['id']
    else:
        return res.text


def get_processor_group_info(processor_group_name):
    """
    Get processor group details
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


def get_processor_id(processor_group_name, processor_name):
    pg_source = get_processor_group_ports(processor_group_name)
    if pg_source.status_code == 200:
        for i in pg_source.json()['processGroupFlow']['flow']['processors']:
            # Get the required processor details
            if i['component']['name'] == processor_name:
                processor_id = i['component']['id']
                return processor_id


def upload_template(template_files):
    root_pg_id = get_nifi_root_pg()
    payload = {'template': open(template_files, 'rb')}
    get_template_upload = requests.post(
        f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_pg_id}/templates/upload", files=payload)
    if get_template_upload.ok:
        print('Successfully uploaded the template', template_files)
    else:
        print("Failed to upload the template ", get_template_upload.text)


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
    if processor_group.__contains__('onestep_dataingestion_aws'):
        template_id = get_template_id('onestep_dataingestion_aws')
        data = {
            "templateId": template_id,
            "originX": -550,
            "originY": -520,
            "disconnectedNodeAcknowledged": "false"
        }
    if processor_group.__contains__('adapters'):
        template_id = get_template_id('Run_adapters')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -1400,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('data_moving_oracle'):
        template_id = get_template_id('data_moving_oracle')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -1090,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('data_moving_azure'):
        template_id = get_template_id('data_moving_azure')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -1090,
            "disconnectedNodeAcknowledged": "false"
        }

    elif processor_group.__contains__('data_moving_aws'):
        template_id = get_template_id('data_moving_aws')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -1090,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('data_moving_local'):
        template_id = get_template_id('data_moving_local')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -1090,
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
    if processor_group.__contains__('telemetry_data'):
        template_id = get_template_id('telemetry_data')
        data = {
            "templateId": template_id,
            "originX": -2016,
            "originY": -800,
            "disconnectedNodeAcknowledged": "false"
        }
    if processor_group.__contains__('ingest_dimension_data'):
        template_id = get_template_id('ingest_dimension_data')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -512,
            "disconnectedNodeAcknowledged": "false"
        }
    # if processor_group.__contains__('ingest_all_data'):
    #     template_id = get_template_id('ingest_all_data')
    #     data = {
    #         "templateId": template_id,
    #         "originX": -1296,
    #         "originY": -216,
    #         "disconnectedNodeAcknowledged": "false"
    #     }
    if processor_group.__contains__('ingest_programwise_data'):
        template_id = get_template_id('ingest_programwise_data')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": 10,
            "disconnectedNodeAcknowledged": "false"
        }
    if processor_group.__contains__('ingest_event_grammar'):
        template_id = get_template_id('ingest_event_grammar')
        data = {
            "templateId": template_id,
            "originX": -552,
            "originY": -800,
            "disconnectedNodeAcknowledged": "false"
        }

    if processor_group.__contains__('ingest_all_data'):
        template_id = get_template_id('ingest_all_data')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -800,
            "disconnectedNodeAcknowledged": "false"
        }
    if processor_group.__contains__('diksha_oracle'):
        template_id = get_template_id('diksha_oracle')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -368,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('pm_poshan_oracle'):
        template_id = get_template_id('pm_poshan_oracle')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -368,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('nas_oracle'):
        template_id = get_template_id('nas_oracle')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -624,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('udise_oracle'):
        template_id = get_template_id('udise_oracle')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -624,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('pgi_oracle'):
        template_id = get_template_id('pgi_oracle')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -888,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('nishtha_oracle'):
        template_id = get_template_id('nishtha_oracle')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -888,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('school_attendance_oracle'):
        template_id = get_template_id('school_attendance_oracle')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -104,
            "disconnectedNodeAcknowledged": "false"
        }

    elif processor_group.__contains__('student_assessment_oracle'):
        template_id = get_template_id('student_assessment_oracle')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -104,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('school_Infrastructure_oracle'):
        template_id = get_template_id('school_Infrastructure_oracle')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": 200,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('student_progression_oracle'):
        template_id = get_template_id('student_progression_oracle')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": 200,
            "disconnectedNodeAcknowledged": "false"
        }

    elif processor_group.__contains__('diksha_local'):
        template_id = get_template_id('diksha_local')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -368,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('pm_poshan_local'):
        template_id = get_template_id('pm_poshan_local')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -368,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('nas_local'):
        template_id = get_template_id('nas_local')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -624,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('udise_local'):
        template_id = get_template_id('udise_local')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -624,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('pgi_local'):
        template_id = get_template_id('pgi_local')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -888,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('nishtha_local'):
        template_id = get_template_id('nishtha_local')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -888,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('school_attendance_local'):
        template_id = get_template_id('school_attendance_local')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -104,
            "disconnectedNodeAcknowledged": "false"
        }

    elif processor_group.__contains__('student_assessment_local'):
        template_id = get_template_id('student_assessment_local')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -104,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('school_Infrastructure_local'):
        template_id = get_template_id('school_Infrastructure_local')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": 200,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('student_progression_local'):
        template_id = get_template_id('student_progression_local')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": 200,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('diksha_aws'):
        template_id = get_template_id('diksha_aws')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -368,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('pm_poshan_aws'):
        template_id = get_template_id('pm_poshan_aws')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -368,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('nas_aws'):
        template_id = get_template_id('nas_aws')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -624,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('udise_aws'):
        template_id = get_template_id('udise_aws')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -624,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('pgi_aws'):
        template_id = get_template_id('pgi_aws')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -888,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('nishtha_aws'):
        template_id = get_template_id('nishtha_aws')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -888,
            "disconnectedNodeAcknowledged": "false"
        }

    elif processor_group.__contains__('school_attendance_aws'):
        template_id = get_template_id('school_attendance_aws')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": -104,
            "disconnectedNodeAcknowledged": "false"
        }

    elif processor_group.__contains__('student_assessment_aws'):
        template_id = get_template_id('student_assessment_aws')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": -104,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('school_Infrastructure_aws'):
        template_id = get_template_id('school_Infrastructure_aws')
        data = {
            "templateId": template_id,
            "originX": 592,
            "originY": 200,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('student_progression_aws'):
        template_id = get_template_id('student_progression_aws')
        data = {
            "templateId": template_id,
            "originX": 1080,
            "originY": 200,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('Code_azure'):
        template_id = get_template_id('Run Latest Code azure')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -832,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('Code_aws'):
        template_id = get_template_id('Run Latest Code aws')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -832,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('Code_local'):
        template_id = get_template_id('Run Latest Code local')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -832,
            "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('Code_Oracle'):
        template_id = get_template_id('Run Latest Code Oracle')
        data = {
            "templateId": template_id,
            "originX": -1296,
            "originY": -832,
            "disconnectedNodeAcknowledged": "false"
        }
    get_import_template = requests.post(
        f'{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_pg_id}/template-instance', json=data)
    if get_import_template.ok:
        print(f"Successfully instantiated the {processor_group} in nifi canvas")
    else:
        print(f"Failed to instantiate the {processor_group} in nifi canvas ", get_import_template.text)


# Updating the processor property
def update_processor_property(processor_group_name, processor_name):
    # Get the processors in the processor group
    pg_source = get_processor_group_ports(processor_group_name)
    if pg_source.status_code == 200:
        for i in pg_source.json()['processGroupFlow']['flow']['processors']:
            # Get the required processor details
            if i['component']['name'] == processor_name:
                # Request body creation to update processor property.
                global update_processor_property_body
                if processor_name == 'InvokeHTTPjwt':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "Original"
                                ],
                                "properties": {
                                    "HTTP Method": "GET",
                                    "Remote URL": config['CREDs']['INGESTION_URL'] + "/generatejwt"
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": 'false'
                    }
                if processor_name == 'onestepInvokeHTTP':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "Original",
                                    "No Retry"
                                ],
                                "properties": {
                                    "HTTP Method": "POST",
                                    "Remote URL": config['CREDs']['SPEC_URL'] + "/schedule"
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": 'false'
                    }
                if processor_name == 'InvokeHTTP':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "autoTerminatedRelationships": [
                                    "Original",
                                    "Retry"
                                ],
                                "properties": {
                                    "HTTP Method": "POST",
                                    "Remote URL": config['CREDs']['QUERY_BUILDER_URL'] + "/captureTelemetry"
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": 'false'
                    }

                if processor_name == 'ListAzure':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "schedulingPeriod": processing_time,
                                "schedulingStrategy": "CRON_DRIVEN",
                                "properties": {
                                    "container-name": config['CREDs']['azure_container_name'],
                                    "storage-account-name": config['CREDs']['azure_account_name'],
                                    "storage-account-key": config['CREDs']['azure_account_key'],
                                    "prefix": "process_input/"
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": 'false'
                    }
                if processor_name == 'FetchAzure':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "properties": {
                                    "container-name": config['CREDs']['azure_container_name'],
                                    "storage-account-name": config['CREDs']['azure_account_name'],
                                    "storage-account-key": config['CREDs']['azure_account_key'],
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": 'false'
                    }

                if processor_name == 'update_dimension_directory':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "properties": {
                                    "directory": '/opt/nifi/nifi-current/Sunbird-cQube-processing-ms/impl/c-qube/ingest/' +
                                                 config['CREDs']['instance_type'] + '/dimensions/'
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": 'false'
                    }
                if processor_name == 'update_program_directory':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "properties": {
                                    "directory": '/opt/nifi/nifi-current/Sunbird-cQube-processing-ms/impl/c-qube/ingest/' +
                                                 config['CREDs']['instance_type'] + '/programs/'
                                }
                            },
                            "state": "STOPPED"
                        },
                        "revision": {
                            "clientId": "",
                            "version": i['revision']['version']
                        },
                        "disconnectedNodeAcknowledged": 'false'
                    }

                if processor_name == 'run_adapter_code':
                    if config['CREDs']['instance_type'] == 'NVSK':
                        update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "autoTerminatedRelationships": [
                                        "original"
                                    ],
                                    "properties": {
                                        "Command Arguments": "NVSK_data_transformation.sh",
                                        "Command Path": "bash",
                                        "Working Directory": "/opt/nifi/nifi-current/adapter/NVSK"
                                    }
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": 'false'
                        }
                    elif config['CREDs']['instance_type'] == 'VSK':
                        update_processor_property_body = {
                            "component": {
                                "id": i['component']['id'],
                                "name": i['component']['name'],
                                "config": {
                                    "autoTerminatedRelationships": [
                                        "original"
                                    ],
                                    "properties": {
                                        "Command Arguments": "VSK_data_transformation.sh",
                                        "Command Path": "bash",
                                        "Working Directory": "/opt/nifi/nifi-current/adapter/VSK"
                                    }
                                },
                                "state": "STOPPED"
                            },
                            "revision": {
                                "clientId": "",
                                "version": i['revision']['version']
                            },
                            "disconnectedNodeAcknowledged": 'false'
                        }
                if processor_name == 'GenerateFlowFile_adapter' or processor_name == 'GenerateFlowFile_oracle':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "schedulingPeriod": config['CREDs']['adapter_schedule_time'],
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

                elif processor_name == 'FetchS3_dist_rev_aws' or processor_name == 'Puts3_dist_rev_aws' or processor_name == 'FetchS3_block_rev_aws' or processor_name == 'Puts3_block_rev_aws' or processor_name == 'FetchS3_cluster_rev_aws' or processor_name == 'Puts3_cluster_rev_aws' or processor_name == 'FetchS3Object' or processor_name == 'FetchS3Object' or processor_name == 'Puts3Processing1' or processor_name == 'Puts3Processing2' or processor_name == 'Puts3Processing3':
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
                elif processor_name == 'GenerateFlowFile' or processor_name == 'ExecuteProcess':
                    update_processor_property_body = {
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
                elif processor_name == 'Listlocal':
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
                elif processor_name == 'Lists3_cluster_rev_local' or processor_name == 'Lists3_dist_rev_local' or processor_name == 'Lists3_block_rev_local':
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

                elif processor_name == 'FetchS3Object_local'  or processor_name == 'Puts3Processing1_local' or processor_name == 'Puts3Processing2_local' or processor_name == 'Puts3Processing3_local' or processor_name == 'FetchS3_dist_rev_local' or processor_name == 'Puts3_dist_rev_local' or processor_name == 'FetchS3_block_rev_local' or processor_name == 'Puts3_block_rev_local' or processor_name == 'FetchS3_cluster_rev_local' or processor_name == 'Puts3_cluster_rev_local':
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


def start_processor_group(processor_group_name, state):
    header = {"Content-Type": "application/json"}
    pg_source = get_processor_group_info(processor_group_name)
    start_body = {
        "id": pg_source['component']['id'],
        "state": state,
        "disconnectedNodeAcknowledged": False}
    start_response = requests.put(
        f"{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{pg_source['component']['id']}",
        json=start_body, headers=header)
    if start_response.status_code == 200:
        print(f"Successfully {state} {pg_source['component']['name']} Processor Group.")
        return True
    else:
        return start_response.text


def delete_processor_group(processor_group_name):
    headers = {
        'Content-Type': 'application/json',
    }
    processor_group_id = get_processor_group_id(processor_group_name)
    nifi_root_pg_id = get_nifi_root_pg()
    pg_list = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{nifi_root_pg_id}')
    if pg_list.status_code == 200:
        # Iterate over processGroups and find the required processor group details
        for i in pg_list.json()['processGroupFlow']['flow']['processGroups']:
            if i['component']['name'] == processor_group_name:
                params = {
                    'version': i['revision']['version'],
                    'clientId': "",
                    'disconnectedNodeAcknowledged': "false"
                }
                delete_api = requests.delete(f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{processor_group_id}",
                                             params=params, headers=headers)


def run_latest_aws():
    upload_template('data_moving_aws.xml')
    instantiate_template_codes('data_moving_aws.xml')
    update_processor_property('data_moving_aws', 'ListS3Files')
    update_processor_property('data_moving_aws', 'FetchS3Object_aws')
    update_processor_property('data_moving_aws', 'update_program_directory')
    update_processor_property('data_moving_aws', 'update_dimension_directory')


def run_school_attendance_aws():
    upload_template('school_attendance_aws.xml')
    instantiate_template('school_attendance_aws.xml')
    update_processor_property('school_attendance_aws', 'ListS3Files')
    update_processor_property('school_attendance_aws', 'FetchS3Object_aws')
    update_processor_property('school_attendance_aws', 'update_program_directory')
    update_processor_property('school_attendance_aws', 'update_dimension_directory')


def run_student_assessment_aws():
    upload_template('student_assessment_aws.xml')
    instantiate_template('student_assessment_aws.xml')
    update_processor_property('student_assessment_aws', 'ListS3Files')
    update_processor_property('student_assessment_aws', 'FetchS3Object_aws')
    update_processor_property('student_assessment_aws', 'update_program_directory')
    update_processor_property('student_assessment_aws', 'update_dimension_directory')


def run_school_Infrastructure_aws():
    upload_template('school_Infrastructure_aws.xml')
    instantiate_template('school_Infrastructure_aws.xml')
    update_processor_property('school_Infrastructure_aws', 'ListS3Files')
    update_processor_property('school_Infrastructure_aws', 'FetchS3Object_aws')
    update_processor_property('school_Infrastructure_aws', 'update_program_directory')
    update_processor_property('school_Infrastructure_aws', 'update_dimension_directory')


def run_student_progression_aws():
    upload_template('student_progression_aws.xml')
    instantiate_template('student_progression_aws.xml')
    update_processor_property('student_progression_aws', 'ListS3Files')
    update_processor_property('student_progression_aws', 'FetchS3Object_aws')
    update_processor_property('student_progression_aws', 'update_program_directory')
    update_processor_property('student_progression_aws', 'update_dimension_directory')


def run_diksha_aws():
    upload_template('diksha_aws.xml')
    instantiate_template('diksha_aws.xml')
    update_processor_property('diksha_aws', 'ListS3Files')
    update_processor_property('diksha_aws', 'FetchS3Object_aws')
    update_processor_property('diksha_aws', 'update_program_directory')
    update_processor_property('diksha_aws', 'update_dimension_directory')


def run_pm_poshan_aws():
    upload_template('pm_poshan_aws.xml')
    instantiate_template('pm_poshan_aws.xml')
    update_processor_property('pm_poshan_aws', 'ListS3Files')
    update_processor_property('pm_poshan_aws', 'FetchS3Object_aws')
    update_processor_property('pm_poshan_aws', 'update_program_directory')
    update_processor_property('pm_poshan_aws', 'update_dimension_directory')


def run_nas_aws():
    upload_template('nas_aws.xml')
    instantiate_template('nas_aws.xml')
    update_processor_property('nas_aws', 'ListS3Files')
    update_processor_property('nas_aws', 'FetchS3Object_aws')
    update_processor_property('nas_aws', 'update_program_directory')
    update_processor_property('nas_aws', 'update_dimension_directory')


def run_udise_aws():
    upload_template('udise_aws.xml')
    instantiate_template('udise_aws.xml')
    update_processor_property('udise_aws', 'ListS3Files')
    update_processor_property('udise_aws', 'FetchS3Object_aws')
    update_processor_property('udise_aws', 'update_program_directory')
    update_processor_property('udise_aws', 'update_dimension_directory')


def run_pgi_aws():
    upload_template('pgi_aws.xml')
    instantiate_template('pgi_aws.xml')
    update_processor_property('pgi_aws', 'ListS3Files')
    update_processor_property('pgi_aws', 'FetchS3Object_aws')
    update_processor_property('pgi_aws', 'update_program_directory')
    update_processor_property('pgi_aws', 'update_dimension_directory')


def run_nishtha_aws():
    upload_template('nishtha_aws.xml')
    instantiate_template('nishtha_aws.xml')
    update_processor_property('nishtha_aws', 'ListS3Files')
    update_processor_property('nishtha_aws', 'FetchS3Object_aws')
    update_processor_property('nishtha_aws', 'update_program_directory')
    update_processor_property('nishtha_aws', 'update_dimension_directory')


def run_school_attendance_local():
    upload_template('school_attendance_local.xml')
    instantiate_template('school_attendance_local.xml')
    update_processor_property('school_attendance_local', 'Listlocal')
    update_processor_property('school_attendance_local', 'FetchS3Object_local')
    update_processor_property('school_attendance_local', 'update_program_directory')
    update_processor_property('school_attendance_local', 'update_dimension_directory')


def run_student_assessment_local():
    upload_template('student_assessment_local.xml')
    instantiate_template('student_assessment_local.xml')
    update_processor_property('student_assessment_local', 'Listlocal')
    update_processor_property('student_assessment_local', 'FetchS3Object_local')
    update_processor_property('student_assessment_local', 'update_program_directory')
    update_processor_property('student_assessment_local', 'update_dimension_directory')


def run_school_Infrastructure_local():
    upload_template('school_Infrastructure_local.xml')
    instantiate_template('school_Infrastructure_local.xml')
    update_processor_property('school_Infrastructure_local', 'Listlocal')
    update_processor_property('school_Infrastructure_local', 'FetchS3Object_local')
    update_processor_property('school_Infrastructure_local', 'update_program_directory')
    update_processor_property('school_Infrastructure_local', 'update_dimension_directory')


def run_student_progression_local():
    upload_template('student_progression_local.xml')
    instantiate_template('student_progression_local.xml')
    update_processor_property('student_progression_local', 'Listlocal')
    update_processor_property('student_progression_local', 'FetchS3Object_local')
    update_processor_property('student_progression_local', 'update_program_directory')
    update_processor_property('student_progression_local', 'update_dimension_directory')


def run_diksha_local():
    upload_template('diksha_local.xml')
    instantiate_template('diksha_local.xml')
    update_processor_property('diksha_local', 'Listlocal')
    update_processor_property('diksha_local', 'FetchS3Object_local')
    update_processor_property('diksha_local', 'update_program_directory')
    update_processor_property('diksha_local', 'update_dimension_directory')


def run_pm_poshan_local():
    upload_template('pm_poshan_local.xml')
    instantiate_template('pm_poshan_local.xml')
    update_processor_property('pm_poshan_local', 'Listlocal')
    update_processor_property('pm_poshan_local', 'FetchS3Object_local')
    update_processor_property('pm_poshan_local', 'update_program_directory')
    update_processor_property('pm_poshan_local', 'update_dimension_directory')


def run_nas_local():
    upload_template('nas_local.xml')
    instantiate_template('nas_local.xml')
    update_processor_property('nas_local', 'Listlocal')
    update_processor_property('nas_local', 'FetchS3Object_local')
    update_processor_property('nas_local', 'update_program_directory')
    update_processor_property('nas_local', 'update_dimension_directory')


def run_udise_local():
    upload_template('udise_local.xml')
    instantiate_template('udise_local.xml')
    update_processor_property('udise_local', 'Listlocal')
    update_processor_property('udise_local', 'FetchS3Object_local')
    update_processor_property('udise_local', 'update_program_directory')
    update_processor_property('udise_local', 'update_dimension_directory')


def run_pgi_local():
    upload_template('pgi_local.xml')
    instantiate_template('pgi_local.xml')
    update_processor_property('pgi_local', 'Listlocal')
    update_processor_property('pgi_local', 'FetchS3Object_local')
    update_processor_property('pgi_local', 'update_program_directory')
    update_processor_property('pgi_local', 'update_dimension_directory')


def run_nishtha_local():
    upload_template('nishtha_local.xml')
    instantiate_template('nishtha_local.xml')
    update_processor_property('nishtha_local', 'Listlocal')
    update_processor_property('nishtha_local', 'FetchS3Object_local')
    update_processor_property('nishtha_local', 'update_program_directory')
    update_processor_property('nishtha_local', 'update_dimension_directory')


def get_outputports(processor_group_name, output_port):
    pg_source = get_processor_group_ports(processor_group_name)
    for i in pg_source.json()['processGroupFlow']['flow']['outputPorts']:
        if i['component']['name'] == output_port:
            id = i['component']['id']
            return id


def get_inputports(processor_group_name, input_port):
    pg_source = get_processor_group_ports(processor_group_name)
    for i in pg_source.json()['processGroupFlow']['flow']['inputPorts']:
        if i['component']['name'] == input_port:
            id = i['component']['id']
            return id


def connect_ports(processor_group_name, src_groupid, src_id, src_type, des_groupid, des_id, des_type):
    pg_source = get_processor_group_ports(processor_group_name)
    root_id = get_nifi_root_pg()
    for i in pg_source.json()['processGroupFlow']['flow']['connections']:
        json = {
            "revision": {
                "clientId": "",
                "version": i['revision']['version']
            },
            "disconnectedNodeAcknowledged": 'false',
            "component": {
                "name": "",
                "source": {
                    "id": src_id,
                    "groupId": src_groupid,
                    "type": src_type
                },
                "destination": {
                    "id": des_id,
                    "groupId": des_groupid,
                    "type": des_type
                }}}
        connect_response = requests.post(f"{nifi_host}:{nifi_port}/nifi-api/process-groups/{root_id}/connections",
                                         json=json)
        if connect_response.status_code == 200:
            print(f"Successfully connected the processor_groups")
            return True
        else:
            return connect_response.text


def common_processor_groups():
    # upload_template('ingest_dimension_data.xml')
    # instantiate_template('ingest_dimension_data.xml')
    upload_template('ingest_all_data.xml')
    instantiate_template('ingest_all_data.xml')
    # upload_template('ingest_programwise_data.xml')
    # instantiate_template('ingest_programwise_data.xml')
    # upload_template('ingest_dimension_grammar.xml')
    # instantiate_template('ingest_dimension_grammar.xml')
    # upload_template('ingest_event_grammar.xml')
    # instantiate_template('ingest_event_grammar.xml')

    # src_id = get_outputports('ingest_dimension_grammar', 'dimension_outputport')
    # des_id = get_inputports('ingest_event_grammar', 'event_inputport')
    # src_groupid = get_processor_group_id('ingest_dimension_grammar')
    # des_groupid = get_processor_group_id('ingest_event_grammar')
    # connect_ports('ingest_dimension_grammar', src_groupid, src_id, "OUTPUT_PORT", des_groupid, des_id, "INPUT_PORT")


def run_latest_local():
    upload_template('data_moving_local.xml')
    instantiate_template_codes('data_moving_local.xml')
    update_processor_property('data_moving_local', 'Listlocal')
    update_processor_property(('data_moving_local'),'FetchS3Object_local')
    update_processor_property('data_moving_local', 'update_dimension_directory')
    update_processor_property('data_moving_local', 'update_program_directory')


def adapters(): 
    upload_template('Run_adapters.xml')
    instantiate_template_codes('Run_adapters.xml')
    update_processor_property('Run_adapters', 'GenerateFlowFile_adapter')
    update_processor_property('Run_adapters', 'run_adapter_code')


def default_pg():
    adapters()
    upload_template('ingest_dimension_dataset_schemas.xml')
    upload_template('ingest_data.xml')
    instantiate_template_codes('ingest_dimension_dataset_schemas.xml')
    instantiate_template_codes('ingest_data.xml')
    update_processor_property('ingest_dimension_dataset_schemas', 'ExecuteProcess')
    update_processor_property('ingest_data', 'ExecuteProcess')


def run_all_programs_oracle():
    upload_template('school_attendance_oracle.xml')
    instantiate_template('school_attendance_oracle.xml')
    update_processor_property('school_attendance_oracle', 'GenerateFlowFile_oracle')

    upload_template('student_assessment_oracle.xml')
    instantiate_template('student_assessment_oracle.xml')
    update_processor_property('student_assessment_oracle', 'GenerateFlowFile_oracle')

    upload_template('school_Infrastructure_oracle.xml')
    instantiate_template('school_Infrastructure_oracle.xml')
    update_processor_property('school_Infrastructure_oracle', 'GenerateFlowFile_oracle')

    upload_template('student_progression_oracle.xml')
    instantiate_template('student_progression_oracle.xml')
    update_processor_property('student_progression_oracle', 'GenerateFlowFile_oracle')

    upload_template('diksha_oracle.xml')
    instantiate_template('diksha_oracle.xml')
    update_processor_property('diksha_oracle', 'GenerateFlowFile_oracle')

    upload_template('pm_poshan_oracle.xml')
    instantiate_template('pm_poshan_oracle.xml')
    update_processor_property('pm_poshan_oracle', 'GenerateFlowFile_oracle')

    upload_template('nas_oracle.xml')
    instantiate_template('nas_oracle.xml')
    update_processor_property('nas_oracle', 'GenerateFlowFile_oracle')

    upload_template('udise_oracle.xml')
    instantiate_template('udise_oracle.xml')
    update_processor_property('udise_oracle', 'GenerateFlowFile_oracle')

    upload_template('pgi_oracle.xml')
    instantiate_template('pgi_oracle.xml')
    update_processor_property('pgi_oracle', 'GenerateFlowFile_oracle')

    upload_template('nishtha_oracle.xml')
    instantiate_template('nishtha_oracle.xml')
    update_processor_property('nishtha_oracle', 'GenerateFlowFile_oracle')


def oracle():
    upload_template('data_moving_oracle.xml')
    instantiate_template_codes('data_moving_oracle.xml')
    update_processor_property('data_moving_oracle', 'GenerateFlowFile_oracle')


def telemetry():
    upload_template('telemetry_data.xml')
    instantiate_template('telemetry_data.xml')
    update_processor_property('telemetry_data', 'GenerateFlowFile')
    update_processor_property('telemetry_data', 'InvokeHTTP')
    update_processor_property('telemetry_data', 'InvokeHTTPjwt')
    start_processor_group('telemetry_data', 'RUNNING')


def azure():
    upload_template('data_moving_azure.xml')
    instantiate_template_codes('data_moving_azure.xml')
    update_processor_property('data_moving_azure', 'ListAzure')
    update_processor_property('data_moving_azure', 'FetchAzure')
    update_processor_property('data_moving_azure', 'update_program_directory')
    update_processor_property('data_moving_azure', 'update_dimension_directory')


def onestep_aws():
    upload_template('onestep_dataingestion_aws.xml')
    instantiate_template_codes('onestep_dataingestion_aws.xml')
    update_processor_property('onestep_dataingestion_aws', 'ListS3Files')
    update_processor_property('onestep_dataingestion_aws', 'FetchS3Object_aws')
    update_processor_property('onestep_dataingestion_aws', 'update_program_directory')
    update_processor_property('onestep_dataingestion_aws', 'update_dimension_directory')
    update_processor_property('onestep_dataingestion_aws', 'onestepInvokeHTTP')


if __name__ == '__main__':
    common_processor_groups()
    adapters()
    telemetry()
    if config['CREDs']['storage_type'] == 'aws':
        run_latest_aws()
        onestep_aws()
        if config['CREDs']['instance_type'] != 'others':
            run_school_attendance_aws()
            run_school_Infrastructure_aws()
            run_student_assessment_aws()
            run_student_progression_aws()
            run_diksha_aws()
            run_nas_aws()
            run_udise_aws()
            run_nishtha_aws()
            run_pm_poshan_aws()
            run_pgi_aws()
    if config['CREDs']['storage_type'] == 'local':
        run_latest_local()
        if config['CREDs']['instance_type'] != 'others':
            run_school_attendance_local()
            run_school_Infrastructure_local()
            run_student_assessment_local()
            run_student_progression_local()
            run_diksha_local()
            run_nas_local()
            run_udise_local()
            run_nishtha_local()
            run_pm_poshan_local()
            run_pgi_local()
    if config['CREDs']['storage_type'] == 'oracle':
        oracle()
        if config['CREDs']['instance_type'] != 'others':
            run_all_programs_oracle()
    if config['CREDs']['storage_type'] == 'azure':
        azure()
    if config['CREDs']['data_pull'] == 'true':
        if config['CREDs']['storage_type'] == 'aws':
            onestep_aws()