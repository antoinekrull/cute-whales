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

    start_date = pd.to_datetime("1980-01-01")
    #  drops all entries before 'start_date'
    temperature_data = temperature_data[temperature_data["datetime"] >= start_date]

    temperature_data.to_csv(TEMPERATURE_CLEAN_DATASET_PATH, encoding="ISO-8859-1")

def ber_import_clean_death_data():
    death_data = pd.read_csv(DEATH_BERLIN_DATASET_PATH)
    death_data.to_csv(DEATH_BERLIN_CLEAN_DATASET_PATH, encoding="ISO-8859-1")

def import_ber_deaths_csv_to_mongodb(**kwargs):
    client = MongoClient(f"mongodb://{MONGODB_IP}:{kwargs['mongodb_port']}")

    #  here to ensure that each time a fresh collection is created in the container
    #  the purpose of that is to make safe that during development new changes can be
    #  seen straight away
    db = client[kwargs['db_name']]

    collection = db[kwargs['collection_name']]
    collection.drop()

    with open(kwargs['csv_file'], 'r') as file:
        lines = file.readlines()
        #  skips the first 6 lines because of unnecessary information
        for row in lines[6:]:
            split_row = row.split(";")
            document = {
                "year": split_row[0],
                "month": get_number_of_month(split_row[1]),
                "region": "Berlin",
                #  drops the "\n" at the end of the total number
                "total deaths": split_row[4][:-2]
            }
            collection.insert_one(document)
    
def get_number_of_month(month):
    if month == "Januar" or month == "01":
        return 1
    elif month == "Februar" or month == "02":
        return 2
    elif month == "März" or month == "03":
        return 3
    elif month == "April" or month == "04":
        return 4
    elif month == "Mai" or month == "05":
        return 5
    elif month == "Juni" or month == "06":
        return 6
    elif month == "Juli" or month == "07":
        return 7
    elif month == "August" or month == "08":
        return 8
    elif month == "September" or month == "09":
        return 9
    elif month == "Oktober" or month == "10":
        return 10
    elif month == "November" or month == "11":
        return 11
    else:
        return 12

def import_temperature_csv_to_mongodb(mongodb_port, csv_file, db_name, collection_name):
    client = MongoClient(f"mongodb://{MONGODB_IP}:{mongodb_port}")

    #  here to ensure that each time a fresh collection is created in the container
    #  the purpose of that is to make safe that during development new changes can be
    #  seen straight away
    db = client[db_name]

    collection = db[collection_name]
    collection.drop()

    with open(csv_file, 'r') as file:
        lines = file.readlines()
        for row in lines:
            split_row = row.split(",")
            document = {
                #  returns the year chars from the datetime string
                "year": split_row[0][:4],
                #  returns the month chars from the datetime string
                "month": split_row[0][5:7],
                "region": split_row[2],
                "temperature": split_row[1],
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
    db = client[db_name]

    collection = db[collection_name]
    collection.drop()

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

def wrangle_fr_death_data_in_mongodb(mongo_port, db_name, collection_ingestion, collection_staging):
    client = MongoClient(f"mongodb://{MONGODB_IP}:{mongo_port}")
    #  here to ensure that each time a fresh collection is created in the container
    #  the purpose of that is to make safe that during development new changes can be
    #  seen straight away
    db = client[db_name]
    stag_col = db[collection_staging]
    stag_col.drop()

    ing_coll = db.get_collection(collection_ingestion)

    # counts the number of people who died in each month of each year
    pipeline = [
            {
                '$group': {
                    '_id': {
                        'Year': '$Year',
                        'Month': '$Month'
                    },
                    'totalDeaths': {'$sum': 1},
                },
            },
            {
                '$project': {
                    '_id': 0,
                    'year': '$_id.Year',
                    'month': '$_id.Month',
                    'totalDeaths': 1
                },
            },
        ]
    result = list(ing_coll.aggregate(pipeline))
    
    # inserts the results of the pipeline into the staging collection as seperate documents
    for r in result:
        document = {
            "year" : r['year'],
            "month" : r['month'],
            "region" : "Paris",
            "total deaths" :  r['totalDeaths']
        }
        stag_coll.insert_one(document)
        

#  operator definition
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

import_ber_death_data_to_mongodb = PythonOperator(
            task_id='import_ber_deaths_to_mongodb',
            dag=dag,
            python_callable=import_ber_deaths_csv_to_mongodb,
            op_kwargs={'mongodb_port': 27017, 'db_name': "temperature_deaths", 'collection_name': "ber_deaths", 'csv_file': "./staging/deaths_berlin.csv"},
            trigger_rule='all_success',
            depends_on_past=False,
        )

ximport_temperature_csv_to_mongodb = PythonOperator(
            task_id='import_temperature_to_mongodb',
            dag=dag,
            python_callable=import_temperature_csv_to_mongodb,
            op_kwargs={'mongodb_port': 27017, 'db_name': "temperature_deaths", 'collection_name': "temperature", 'csv_file': "./staging/GlobalLandTemperaturesByMajorCity.csv"},
            trigger_rule='all_success',
            depends_on_past=False,
        )

xfr_get_death_files_list = PythonOperator(
            task_id='fr_get_death_files_list',
            dag=dag,
            python_callable=fr_get_death_files_list,
            op_kwargs={},
            trigger_rule='all_success',
            depends_on_past=False,
        )

xfr_get_all_death_files = PythonOperator(
            task_id='fr_get_all_death_files',
            dag=dag,
            python_callable=fr_get_all_death_files,
            op_kwargs={},
            trigger_rule='all_success',
            depends_on_past=False,
        )

xfr_collect_specific_location_data = PythonOperator(
            task_id='fr_collect_specific_location_data',
            dag=dag,
            python_callable=fr_collect_specific_location_data,
            op_kwargs={},
            trigger_rule='all_success',
            depends_on_past=False,
        )

xfr_death_data_to_csv = PythonOperator(
            task_id='fr_death_data_to_csv',
            dag=dag,
            python_callable=fr_death_data_to_csv,
            op_kwargs={},
            trigger_rule='all_success',
            depends_on_past=False,
        )

ximport_fr_deaths_csv_to_mongodb = PythonOperator(
            task_id='import_fr_deaths_csv_to_mongodb',
            dag=dag,
            python_callable=import_fr_deaths_csv_to_mongodb,
            op_kwargs={},
            trigger_rule='all_success',
            depends_on_past=False,
        )

xwrangle_fr_death_data_in_mongodb = PythonOperator(
            task_id='wrangle_fr_death_data_in_mongodb',
            dag=dag,
            python_callable=wrangle_fr_death_data_in_mongodb,
            op_kwargs={},
            trigger_rule='all_success',
            depends_on_past=False,
        )

start >> [get_temperature_data, get_ber_death_data, fr_get_death_files_list]
get_ber_death_data >> import_ber_death_data_to_mongodb
get_temperature_data >> ximport_temperature_csv_to_mongodb
xfr_get_death_files_list >> xfr_get_all_death_files >> xfr_collect_specific_location_data >> xfr_death_data_to_csv >> ximport_fr_deaths_csv_to_mongodb >> xwrangle_fr_death_data_in_mongodb