import datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
from pymongo import MongoClient
import csv

#  constants
TEMPERATURE_DATASET_PATH = "./ingestion/GlobalLandTemperaturesByMajorCity.json"
TEMPERATURE_CLEAN_DATASET_PATH = "./staging/GlobalLandTemperaturesByMajorCity.csv"
#  TODO: has to be evaluated by synne
MONGODB_IP = "127.0.0.0"
MONGODB_PORT = "27017"
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

def import_csv_to_mongodb(csv_file, db_name, collection_name):
    client = MongoClient(MONGODB_IP, MONGODB_PORT)

    db = client[db_name]
    collection = db[collection_name]

    with open(csv_file, 'r') as file:
        csvreader = csv.DictReader(file)
        for row in csvreader:
            collection.insert_one(row)

#  operator definition
