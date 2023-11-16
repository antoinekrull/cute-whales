import datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
from pymongo import MongoClient

#  constants
TEMPERATURE_DATASET_PATH = "./ingestion/GlobalLandTemperaturesByMajorCity.json"
DEATH_BERLIN_DATASET_PATH = "./ingestion/deaths_berlin.csv"
DEATH_BERLIN_CLEAN_DATASET_PATH = "./staging/deaths_berlin.csv"
TEMPERATURE_CLEAN_DATASET_PATH = "./staging/GlobalLandTemperaturesByMajorCity.csv"
FR_DEATH_DATASET_URL = 'https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5'
FR_DEATH_INGESTION_DATA_PATH = './ingestion/fr/'
FR_DEATH_CLEAN_DATA_PATH = './staging/'
PARIS_GEOGRAPHIC_CODE = '75'
MONGODB_IP = "127.0.0.1"

#  DAG definition
default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=10),
}

dag = DAG(
    dag_id='dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

#  functions
def import_clean_temperature_data():
    temperature_data = pd.read_json(TEMPERATURE_DATASET_PATH)
    temperature_data["dt"] = pd.to_datetime(temperature_data["dt"])
    temperature_data = temperature_data.drop(columns={"AverageTemperatureUncertainty"})
    temperature_data = temperature_data.rename(columns={"dt": "datetime"})
    #  replaces empty AT fields with 0
    #  TODO: think of smarter value
    temperature_data['AverageTemperature'].replace('', 0, inplace=True)
    #  i am not sure why this is not working correctly
    #  temperature_data.round({"AverageTemperature": 2})

    start_date = pd.to_datetime("1900-01-01")
    #  drops all entries before 'start_date'
    temperature_data = temperature_data[temperature_data["datetime"] >= start_date]

    temperature_data.to_csv(TEMPERATURE_CLEAN_DATASET_PATH, encoding="ISO-8859-1")

def ber_import_clean_death_data():
    death_data = pd.read_csv(DEATH_BERLIN_DATASET_PATH)
    death_data.to_csv(DEATH_BERLIN_CLEAN_DATASET_PATH, encoding="ISO-8859-1")

def import_deaths_csv_to_mongodb(mongodb_port, csv_file, db_name, collection_name):
    client = MongoClient(f"mongodb://{MONGODB_IP}:{mongodb_port}")

    #  here to ensure that each time a fresh collection is created in the container
    #  the purpose of that is to make safe that during development new changes can be
    #  seen straight away
    my_col = db[collection_name]
    my_col.drop()

    db = client[db_name]
    collection = db[collection_name]

    with open(csv_file, 'r') as file:
        lines = file.readlines()
        #  skips the first 6 lines because of unnecessary information
        for row in lines[6:]:
            split_row = row.split(";")
            document = {
                "year": split_row[0],
                "month": get_number_of_month(split_row[1]),
                "region": "Berlin",
                #  drops the "\n" at the end of the total number
                "total": split_row[4][:-2]
            }
            collection.insert_one(document)
    
def get_number_of_month(month):
    month_dict = {
        "Januar": 1,
        "Februar": 2,
        "März": 3,
        "April": 4,
        "Mai": 5,
        "Juni": 6,
        "Juli": 7,
        "August": 8,
        "September": 9,
        "Oktober": 10,
        "November": 11,
        "Dezember": 12
    }
    return month_dict.get(month)

def import_temperature_csv_to_mongodb(mongodb_port, csv_file, db_name, collection_name):
    client = MongoClient(f"mongodb://{MONGODB_IP}:{mongodb_port}")

    #  here to ensure that each time a fresh collection is created in the container
    #  the purpose of that is to make safe that during development new changes can be
    #  seen straight away
    my_col = db[collection_name]
    my_col.drop()

    db = client[db_name]
    collection = db[collection_name]

    with open(csv_file, 'r') as file:
        lines = file.readlines()
        for row in lines:
            split_row = row.split(",")
            document = {
                "datetime": split_row[0],
                "AverageTemperature": split_row[1],
                "City": split_row[2],
                "Country": split_row[3]
            }
            collection.insert_one(document)
            
def fr_get_death_files_list():
    import requests
    with open(f'{FR_DEATH_INGESTION_DATA_PATH}urls.txt', 'w') as f:
        resources = requests.get(FR_DEATH_DATASET_URL).json()['resources']
        for r in resources:
            f.write(r['latest']+ '\n')

def fr_get_all_death_files():
    import requests
    with open(f'{FR_DEATH_INGESTION_DATA_PATH}urls.txt', 'r') as f:
        for i, line in enumerate(f): 
            with open(f'{FR_DEATH_INGESTION_DATA_PATH}data{i}.txt', 'w') as f1:
                try:
                    res= requests.get(line.strip())
                    f1.write(res.content.decode('UTF-8'))
                    #time.sleep(1)
                except(UnicodeEncodeError):
                        print(f"In file {i} occured an encoding error")
                except(UnicodeDecodeError):
                        print(f"In file {i} occured an decoding error")

def fr_collect_specific_location_data(PARIS_GEOGRAPHIC_CODE):
    import glob
    location = PARIS_GEOGRAPHIC_CODE
    files = glob.glob(f'{FR_DEATH_INGESTION_DATA_PATH}*.txt')
    for f in files:
        with open(f'{FR_DEATH_INGESTION_DATA_PATH}f', 'r') as f2:
            for line in f2:
                death_location = line[162:167]
                if (death_location[:2] == location):
                    with open(f'{FR_DEATH_INGESTION_DATA_PATH}data.txt','a') as f3:
                            name = line[:80].strip().strip('/').replace('*', ' ')
                            death_date = line[154:162]
                            f3.write(f'{name}, {death_date}, {death_location} \n')

def fr_death_data_to_csv():
    account = pd.read_csv(f'{FR_DEATH_INGESTION_DATA_PATH}', header= None)
    account.columns = ['Name', 'Date of death', 'Location of Death']
    account.to_csv(f'{FR_DEATH_CLEAN_DATA_PATH}ParisDeathData.csv', index= None)

def import_fr_deaths_csv_to_mongodb(mongodb_port, csv_file, db_name, collection_name):
    client = MongoClient(f"mongodb://{MONGODB_IP}:{mongodb_port}")

    #  here to ensure that each time a fresh collection is created in the container
    #  the purpose of that is to make safe that during development new changes can be
    #  seen straight away
    my_col = db[collection_name]
    my_col.drop()

    db = client[db_name]
    collection = db[collection_name]

    with open(csv_file, 'r') as file:
        # skip row with column titles
        lines = file.readlines()
        for row in lines[1:]:
            split_row = row.split(",")
            document = {
                "Name": split_row[0],
                "Year": split_row[1][:4],
                "Month": split_row[1][4:6],
                "Location": "Paris",
            }
            collection.insert_one(document)

#  operator definition

with TaskGroup("ingestion_pipeline","data ingestion step",dag=dag) as ingestion_pipeline:
    start = DummyOperator(
            task_id='start',
            dag=dag,
        )

    get_temperature_data = PythonOperator(
            task_id='get_clean_temperature',
            dag=dag,
            python_callable=import_clean_temperature_data,
            op_kwargs={},
            trigger_rule='all_success',
            depends_on_past=False,
        )
    
    get_ber_death_data = PythonOperator(
            task_id='get_ber_deaths',
            dag=dag,
            python_callable=ber_import_clean_death_data,
            op_kwargs={},
            trigger_rule='all_success',
            depends_on_past=False,
        )
