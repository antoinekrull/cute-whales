import datetime

import pandas as pd

from sqlalchemy import create_engine
from py2neo import Graph

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args_dict = {
    "start_date": datetime.datetime(2023, 11, 13, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 7 * * *",  
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

# Initialize Airflow DAG
production_dag = DAG(
    dag_id = "production_dag",
    default_args = default_args_dict,
    catchup = False,
)

# execute start-command in a Bash shell
start_node = BashOperator(
    task_id = "start_task",
    bash_command = "echo 'Start Task'",
    dag  =production_dag,
    trigger_rule = "all_success",
)

# function to extract columns from postgres-database and save to neo4j graph-database on desired format
def load_into_neo4j(
    pg_user: str,
    pg_pwd: str,
    pg_host: str,
    pg_port: str,
    pg_db: str,
    neo_host: str,
    neo_port: str,
):
    
    query = """
        SELECT year, month, total_deaths, region, temperature
        FROM death_temperature_table
    """
    # connect to PostgreSQL database
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}'
    )
    # fetch query-data from PostgreSQL database
    df = pd.read_sql(query, con=engine)
    print(df.columns.values)
    engine.dispose()

    # connect to Neo4j database
    graph = Graph(f"bolt://{neo_host}:{neo_port}")

    graph.delete_all()
    tx = graph.begin()
    # TODO: mabye create spans for the temperature and deaths
    # iterate over the rows from postgres and chenage format to fit into neo4j
    for _, row in df.iterrows():
        tx.evaluate('''
        MERGE (t:Temperature {value: $temperature})
        MERGE (d:Death {value: $total_deaths})
        MERGE (t)-[r:IN_REGION {region: $region, year: $year, month: $month}]->(d)
        ''', parameters={
            'temperature': row['temperature'],
            'total_deaths': row['total_deaths'],
            'region': row['region'],
            'year': row['year'],
            'month': row['month'],
        })
    tx.commit()

# execute Python function to insert data into neo4j
graph_node = PythonOperator(
    task_id = "saving_to_neo4j",
    dag = production_dag,
    trigger_rule = "all_success",
    python_callable = load_into_neo4j,
    op_kwargs={
        "pg_user": "airflow",
        "pg_pwd": "airflow",
        "pg_host": "postgres",
        "pg_port": "5432",
        "pg_db": "postgres",
        "neo_host": "neo4j",
        "neo_port": "7474",
    },
) 

# funtion to execute the two queries corresponding to our project questions
def execute_query(
    neo_host: str, 
    neo_port: str
):
    # connect to neo4j localhost port 7474
    graph = Graph(f"bolt://{neo_host}:{neo_port}")

    # Query 1: Find temperatures linked to deaths by region
    query1 = '''
        MATCH (t:Temperature)-[r:IN_REGION]->(d:Deaths)
        RETURN t.value, d.value, r.region, r.year, r.month
        ORDER BY d.value DESC;
    '''

    # Query 2: Find temperatures linked to deaths with extreme events
    query2 = '''
        MATCH (t:Temperature)<-[r:IN_REGION]-(d:Deaths)
        WHERE d.total_deaths > threshold // specify the threshold for extreme events
        RETURN t.value, d.value, r.region, r.year, r.month
        ORDER BY d.total_deaths DESC;
    '''

    # run queries and store the results in DataFrames
    out1 = graph.run(query1).to_data_frame()
    out2 = graph.run(query2).to_data_frame()

    # Save the result as a CSV file or perform any other necessary action
    out1.to_csv('output1.csv', index=False)
    out2.to_csv('output2.csv', index=False)

# Python operator to query the graph-database and visualize output in a .csv-file
execute_query_node = PythonOperator(
    task_id = "query_ne4j",
    dag = production_dag,
    trigger_rule = "all_success",
    python_callable = execute_query,
    op_kwargs={
        "neo_host": "localhost",
        "neo_port": "7687",
    },
)

# execute end-command in a Bash shell
end_node = BashOperator(
    task_id = "end_task",
    bash_command = "echo 'End Task'",
    dag = production_dag,
    trigger_rule = "all_success",
)

# DAG-order
start_node >> graph_node >> execute_query_node >> end_node