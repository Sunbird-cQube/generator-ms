import configparser
import requests
import os


configuartion_path = os.path.dirname(os.path.abspath(__file__))+"/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

nifi_host = config['CREDs']['nifi_host']
nifi_port = config['CREDs']['nifi_port']
nifi_schedule_latest_code = config['CREDs']['processing_time']
s3_access_key = config['CREDs']['s3_access_key']
s3_secret_key = config['CREDs']['s3_secret_key']
s3_input_bucket = config['CREDs']['s3_input_bucket']
header = {"Content-Type": "application/json"}

def get_nifi_root_pg():
    """ Fetch nifi root processor group ID"""
    res = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/process-groups/root',  verify=True)
    if res.status_code == 200:
        global nifi_root_pg_id
        nifi_root_pg_id = res.json()['component']['id']
        print(nifi_root_pg_id)
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


def instantiate_template(processor_group):
    # Instantiates template
    root_pg_id = get_nifi_root_pg()
    data = {}
    if processor_group.__contains__('Code'):
        template_id = get_template_id('Run Latest Code')
        data = {
        "templateId": template_id,
        "originX": -1067.5854405025766,
        "originY": -1529.7644241816233,
        "disconnectedNodeAcknowledged": "false"
        }
    elif processor_group.__contains__('Plugin'):
        template_id = get_template_id('Plugin to Split CSV')
        data = {
            "templateId": template_id,
            "originX": -1100.5854405025766,
            "originY": -1700.7644241816233,
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
                            "Access Key": s3_access_key,
                            "Secret Key": s3_secret_key
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
                if processor_name == 'Lists3' or processor_name == 'FetchS3Object' or processor_name == 'FetchS3Object' or  processor_name == 'Puts3Processing1'or  processor_name == 'Puts3Processing2'or  processor_name == 'Puts3Processing3':
                    update_processor_property_body = {
                        "component": {
                            "id": i['component']['id'],
                            "name": i['component']['name'],
                            "config": {
                                "properties": {
                                    "Bucket": s3_input_bucket
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
                                "schedulingPeriod": nifi_schedule_latest_code,
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

if __name__ == '__main__':
    upload_template('Plugin_to_Split_CSV.xml')
    instantiate_template('Plugin_to_Split_CSV.xml')
    upload_template('Run_Latest_Code.xml')
    instantiate_template('Run_Latest_Code.xml')
    update_processor_property('Run Latest Code','GenerateFlowFile')
    controller_service_disable('Plugin to Split CSV')
    update_controller_service_property('Plugin to Split CSV', 'AWSCredentialsProviderControllerService')
    controller_service_enable('Plugin to Split CSV')
    processors = ['Lists3','FetchS3Object', 'FetchS3Object','Puts3Processing1','Puts3Processing2','Puts3Processing3']
    for i in processors:
        update_processor_property('Plugin to Split CSV',i)
    start_processor_group('Run Latest Code', 'RUNNING')
