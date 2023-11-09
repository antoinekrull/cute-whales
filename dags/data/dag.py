import datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
from pymongo import MongoClient

#  constants
TEMPERATURE_DATASET_PATH = "./ingestion/GlobalLandTemperaturesByMajorCity.json"
DEATH_BERLIN_DATASET_PATH = "./ingestion/deaths_berlin.csv"
DEATH_BERLIN_CLEAN_DATASET_PATH = "./staging/deaths_berlin.csv"
TEMPERATURE_CLEAN_DATASET_PATH = "./staging/GlobalLandTemperaturesByMajorCity.csv"
MONGODB_IP = "127.0.0.1"
#  DAG definition
default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=10),
}

global_dag = DAG(
    dag_id='global_dag',
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

def import_clean_death_data():
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
                "month": split_row[1],
                #  drops the "\n" at the end of the total number
                "total": split_row[4][:-2]
            }
            collection.insert_one(document)

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
                "Country": split_row[3],
                "Latitude": split_row[3],
                "Longitude": split_row[4]
            }
            collection.insert_one(document)

#  operator definition
