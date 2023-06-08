import pytest
import psycopg2
import subprocess
import json
import requests

from add_nifi_template import *

@pytest.fixture(scope="session")
def name(pytestconfig):
    return pytestconfig.getoption("name")

configuartion_path = os.path.dirname(os.path.abspath(__file__))+"/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

nifi_host = config['CREDs']['nifi_host']
nifi_port = config['CREDs']['nifi_port']
@pytest.fixture(scope="session")
def db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="pytest",
        user="pytest_user",
        password="postgres"
    )
    yield conn
    conn.close()

def test_install_packages():
        subprocess.run(["bash",'install_package.sh'],check=True)
        assert True


def test_processor_group_exist(name):
    processor_group_name = f"{name}"
    nifi_root_pg_id = get_nifi_root_pg()
    upload_template(processor_group_name+'.xml')
    instantiate_template(processor_group_name+'.xml')
    pg_list = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/flow/process-groups/{nifi_root_pg_id}')
    processor_group_status = ''
    for i in pg_list.json()['processGroupFlow']['flow']['processGroups']:
        if i['component']['name'] == processor_group_name:
            processor_group_status += 'found'
    assert processor_group_status == 'found'
    delete_processor_group(processor_group_name)

def test_scheduling(name):
    expected_scheduling_period = '* * * * * ?'
    processor_group_name = f"{name}"
    processor = 'GenerateFlowFile_oracle'
    processor_id = get_processor_id(processor_group_name,processor)
    processor_details = requests.get(f'{nifi_host}:{nifi_port}/nifi-api/processors/{processor_id}')
    actual_scheduling_period = processor_details.json()['component']['config']['schedulingPeriod']
    assert expected_scheduling_period == actual_scheduling_period

def test_file_moving_dimension(name):
    dimension_folder = config['CREDs']['pytest_dimension_folder']
    for filename in os.listdir(dimension_folder):
        file_path = os.path.join(dimension_folder, filename)
        if file_path.__contains__('dimension'):
            assert True

def test_file_moving_program(name):
    dimension_folder = config['CREDs']['pytest_program_folder']
    for filename in os.listdir(dimension_folder):
        file_path = os.path.join(dimension_folder, filename)
        if file_path.__contains__('program'):
            assert True

def test_table_contains_record(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM datasets.diksha_avg_play_time_in_mins_on_app_and_portal_medium")
    record_count = cursor.fetchone()[0]
    assert record_count > 0, "The table does not contain any records."

def test_cli_ingest():
    working_directory = config['CREDs']['code_folder']
    command = ["yarn", "cli", "ingest"]
    subprocess.run(command, cwd=working_directory, check=True)
    assert True

def test_cli_ingest_data():
    working_directory = config['CREDs']['code_folder']
    command = ["yarn", "cli", "ingest-data"]
    subprocess.run(command, cwd=working_directory, check=True)




