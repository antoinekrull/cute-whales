import datetime
from os import system
from subprocess import CalledProcessError, check_output, STDOUT

import pandas as pd

from sqlalchemy import create_engine
from py2neo import Graph

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args_dict = {
    "start_date": datetime.datetime(2022, 11, 8, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 4 * * *",  
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=15),
}

production_dag = DAG(
    dag_id="production_dag",
    default_args=default_args_dict,
    catchup=False,
)

start_node = BashOperator(
    task_id="start_task",
    bash_command="echo 'Start Task'",
    dag=production_dag,
    trigger_rule="all_success",
)

def _saving_to_neo4j(
    pg_user: str,
    pg_pwd: str,
    pg_host: str,
    pg_port: str,
    pg_db: str,
    neo_host: str,
    neo_port: str,
):
    query = """
        -- Your SQL Query to fetch data
        SELECT year, month, total_deaths, region, temperature
        FROM death_temperature_table
    """
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}'
    )
    df = pd.read_sql(query, con=engine)
    print(df.columns.values)
    engine.dispose()

    graph = Graph(f"bolt://{neo_host}:{neo_port}")

    graph.delete_all()
    tx = graph.begin()
    for _, row in df.iterrows():
        print(f"Processing data for {row['region']} in {row['month']} {row['year']}")
        tx.evaluate('''
        MERGE (c:City {name: $city_name, region: $region})
        MERGE (t:Temperature {value: $temperature})
        MERGE (c)-[r:HAS_DEATHS {year: $year, month: $month, deaths: $total_deaths}]->(t)
        ''', parameters={
            'city_name': row['region'],
            'region': row['region'],
            'temperature': row['temperature'],
            'year': row['year'],
            'month': row['month'],
            'total_deaths': row['total_deaths'],
        })
    tx.commit()

graph_node = PythonOperator(
    task_id="saving_to_neo4j",
    dag=production_dag,
    trigger_rule="all_success",
    python_callable=_saving_to_neo4j,
    op_kwargs={
        "pg_user": "your_pg_user",
        "pg_pwd": "your_pg_password",
        "pg_host": "your_pg_host",
        "pg_port": "your_pg_port",
        "pg_db": "your_pg_database",
        "neo_host": "your_neo_host",
        "neo_port": "your_neo_port",
    },
)

# TODO: write a task to ask the queries as well as visualize the output of the queries. 

end_node = BashOperator(
    task_id="end_task",
    bash_command="echo 'End Task'",
    dag=production_dag,
    trigger_rule="all_success",
)

start_node >> graph_node >> end_node

# Query question 1:
# MATCH (city:City)-[r:HAS_DEATHS]->(temperature:Temperature)
# RETURN city.name, temperature.value, r.year, r.month, r.number_of_deaths, r.region
# ORDER BY r.number_of_deaths DESC;

# Query question 2:
# MATCH (temperature:Temperature)<-[r:HAS_DEATHS]-(city:City)
# WHERE r.number_of_deaths > threshold // specify the threshold for extreme events
# RETURN city.name, temperature.value, r.year, r.month, r.number_of_deaths, r.region
# ORDER BY r.number_of_deaths DESC;