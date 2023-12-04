import datetime
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2
from py2neo import Graph
from airflow.models import Variable
import math

default_args_dict = {
    "start_date": datetime.datetime(2023, 11, 28, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 7 * * *", 
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=15),
}

query_DAG = DAG(
    dag_id="query_dag",
    default_args=default_args_dict,
    catchup=False,
)

start_node = EmptyOperator(
    task_id="start_task", 
    dag=query_DAG, 
    trigger_rule="all_success"
)
    
# Function to execute SQL query and insert data into Neo4j
def import_data_to_neo4j():
    # Define PostgreSQL connection parameters
    postgres_params = {
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432',
        'database': 'postgres',
    }
    # SQL query to retrieve data from the merged table
    sql_query = "SELECT year, month, total_deaths, region, temperature FROM death_temperature_table"

    # Connect to PostgreSQL database
    try:
        postgres_connection = psycopg2.connect(**postgres_params)
        postgres_cursor = postgres_connection.cursor()

        # Execute SQL query
        postgres_cursor.execute(sql_query)

        # Fetch all rows from the result set
        rows = postgres_cursor.fetchall()

        # Close PostgreSQL cursor and connection
        postgres_cursor.close()
        postgres_connection.close()

        # Connect to Neo4j database
        neo4j_driver = Graph(f"bolt://localhost:7687")
        neo4j_session = neo4j_driver.session()

        # Create nodes and relationships in Neo4j
        for row in rows:
            year, month, total_deaths, region, temperature = row

            # Create or retrieve nodes
            neo4j_session.run(
                """
                MERGE (y:Year {value: $year})
                MERGE (m:Month {value: $month})
                MERGE (td:TotalDeaths {value: $total_deaths})
                MERGE (r:Region {value: $region})
                MERGE (t:Temperature {value: $temperature})
                """,
                year=year, month=month, total_deaths=total_deaths, region=region, temperature=temperature
            )

            # Create relationships
            neo4j_session.run(
                """
                MATCH (t:Temperature {value: $temperature})
                MATCH (m:Month {value: $month})
                CREATE (t)-[:IN_MONTH]->(m)
                """,
                temperature=temperature, month=month
            )

            neo4j_session.run(
                """
                MATCH (t:Temperature {value: $temperature})
                MATCH (r:Region {value: $region})
                CREATE (t)-[:IN_REGION]->(r)
                """,
                temperature=temperature, region=region
            )

            neo4j_session.run(
                """
                MATCH (td:TotalDeaths {value: $total_deaths})
                MATCH (y:Year {value: $year})
                CREATE (td)-[:OCCURRED_IN]->(y)
                """,
                total_deaths=total_deaths, year=year
            )

            neo4j_session.run(
                """
                MATCH (y:Year {value: $year})
                MATCH (m:Month {value: $month})
                CREATE (y)-[:IN_YEAR]->(m)
                """,
                year=year, month=month
            )

        # Close Neo4j session and driver
        neo4j_session.close()
        neo4j_driver.close()

    except Exception as e:
        print(f"Error: {e}")

graph_database_node = PythonOperator(
    task_id="postgres_to_neo4j",
    dag=query_DAG,
    trigger_rule="all_success",
    python_callable=import_data_to_neo4j,
)

def calculate_correlation_one():
    # Retrieve user-defined variables for month and region
    month = Variable.get("correlation_month", default_var="default_month")
    region = Variable.get("correlation_region", default_var="default_region")

    connection_params = {
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres',
    }
    # Connect to the postgres databse
    connection = psycopg2.connect(**connection_params)
    cursor = connection.cursor()

    cursor.execute('''SELECT SUM(temperature) FROM death_temperature_table WHERE month = ? AND region = ?;''', (month, region))
    x_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(total_deaths) FROM death_temperature_table WHERE month = ? AND region = ?;''', (month, region))
    y_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * temperature) FROM death_temperature_table WHERE month = ? AND region = ?;''', (month, region))
    x_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(total_deaths * total_deaths) FROM death_temperature_table WHERE month = ? AND region = ?;''', (month, region))
    y_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT COUNT(*) FROM death_temperature_table WHERE month = ? AND region = ?;''', (month, region))
    count_x = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * total_deaths) FROM death_temperature_table WHERE month = ? AND region = ?;''', (month, region))
    sum_xy_product = cursor.fetchone()[0]
   
   # math function for the correlation
    try:
        correlation_coefficient = (count_x * sum_xy_product - (x_sum * y_sum)) / math.sqrt((count_x * x_squared_sum - x_sum * x_sum) * (count_x * y_squared_sum - y_sum * y_sum))
    # if denominator is zero, error occurs
    except ZeroDivisionError:
        print("Error: Division by zero. This could be due to the dataset being too small or no data points meeting the threshold.")
        correlation_coefficient = None

    # Commit and close the connection
    connection.commit()
    cursor.close()
    connection.close()
    
    return correlation_coefficient

correlation_one_node = PythonOperator(
    task_id="correlation_temperature_death",
    dag=query_DAG,
    trigger_rule="all_success",
    python_callable=calculate_correlation_one,
)

def calculate_correlation_two():
    # Retrieve user-defined variables for month and region
    month = Variable.get("correlation_month", default_var="default_month")
    region = Variable.get("correlation_region", default_var="default_region")
    threshold = Variable.get("threshold_temperature", default_var="default_threshold")

    connection_params = {
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres',
    }
    # Connect to the postgres database
    connection = psycopg2.connect(**connection_params)
    cursor = connection.cursor()

    cursor.execute('''SELECT SUM(temperature) FROM death_temperature_table WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    x_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(total_deaths) FROM death_temperature_table WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    y_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * temperature) FROM death_temperature_table WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    x_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(total_deaths * total_deaths) FROM death_temperature_table WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    y_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT COUNT(*) FROM death_temperature_table WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    count_x = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * total_deaths) FROM death_temperature_table WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    sum_xy_product = cursor.fetchone()[0]
   
    # math function for the correlation
    try:
        correlation_coefficient = (count_x * sum_xy_product - (x_sum * y_sum)) / math.sqrt((count_x * x_squared_sum - x_sum * x_sum) * (count_x * y_squared_sum - y_sum * y_sum))
    # if denominator is zero, error occurs
    except ZeroDivisionError:
        print("Error: Division by zero. This could be due to the dataset being too small or no data points meeting the threshold.")
        correlation_coefficient = None

    # Commit and close the connection
    connection.commit()
    cursor.close()
    connection.close()
    
    return correlation_coefficient

correlation_two_node = PythonOperator(
    task_id="correlation_temperature_death_threshold",
    dag=query_DAG,
    trigger_rule="all_success",
    python_callable=calculate_correlation_two,
)

end_node = EmptyOperator(
    task_id="end_task", 
    dag=query_DAG, 
    trigger_rule="all_success"
)

start_node >> graph_database_node >> correlation_one_node >> correlation_two_node >> end_node