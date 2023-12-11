import datetime
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import math
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2

default_args_dict = {
    "start_date": datetime.datetime(2023, 11, 28, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 7 * * *", 
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=15),
}

query_DAG = DAG(
    dag_id="dag",
    default_args=default_args_dict,
    catchup=False,
)

start_node = EmptyOperator(
    task_id="start_task", 
    dag=query_DAG, 
    trigger_rule="all_success"
)

# Return: List
# Iterates over all the regions in the dataset and all the months of a year and returns a list with the correlation coefficient for 
# each region and month
def calculate_correlation(threshold):
    # Define PostgreSQL connection parameters
    postgres_params = {
        "postgres_conn_id": "postgres_default",
        "host": "localhost",
        "port": 5432,
        "database": "postgres",
        "username": "airflow",
        "password": "airflow",
    }

    # Connect to the postgres databse
    connection = psycopg2.connect(**postgres_params)
    cursor = connection.cursor()

    cursor.execute('''SELECT DISTINCT region FROM deaths_and_temperature;''')
    regions = cursor.fetchall()

    liste = []
    for region in regions:
        region = region[0]

        for month in range(1, 13):
            if threshold == None:
                liste.append({'Month': month, 'Region': region, 'Correlation coefficient': query_without_threshold(cursor, region, month), 'Threshold': None})
            else:
                liste.append({'Month': month, 'Region': region, 'Correlation coefficient': query_with_threshold(cursor, region, month, threshold), 'Treshold': threshold})

    # Commit and close the connection
    connection.commit()
    cursor.close()
    connection.close()

    return liste

correlation_one_node = PythonOperator(
    task_id="visualization",
    dag=query_DAG,
    trigger_rule="all_success",
    # threshold = None
    python_callable=calculate_correlation(None),
)

correlation_two_node = PythonOperator(
    task_id="visualization",
    dag=query_DAG,
    trigger_rule="all_success",
    # threshold = 32.0 °
    python_callable=calculate_correlation(32.0),
)

# Return: correlation coefficient
# For a given month in a given region, queries the database to calcualte the correlation coefficient for a spesific threshold for the temperature
def query_with_threshold(cursor, region, month, threshold):
    
    cursor.execute('''SELECT SUM(temperature) FROM deaths_and_temperature WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    x_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(total_deaths) FROM deaths_and_temperature WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    y_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * temperature) FROM deaths_and_temperature WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    x_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(total_deaths * total_deaths) FROM deaths_and_temperature WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    y_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT COUNT(*) FROM deaths_and_temperature WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    count_x = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * total_deaths) FROM deaths_and_temperature WHERE month = ? AND region = ? AND temperature >= ?;''', (month, region, threshold))
    sum_xy_product = cursor.fetchone()[0]
   
    # math function for the correlation
    try:
        correlation_coefficient = (count_x * sum_xy_product - (x_sum * y_sum)) / math.sqrt((count_x * x_squared_sum - x_sum * x_sum) * (count_x * y_squared_sum - y_sum * y_sum))
    # if denominator is zero, error occurs
    except ZeroDivisionError:
        print("Error: Division by zero. This could be due to the dataset being too small or no data points meeting the threshold.")
        correlation_coefficient = None

    liste = []
    liste.append({'Month': month, 'Region': region, 'Correlation coefficient': correlation_coefficient, 'Threshold': threshold})
    return print(liste)

# Return: correlation coefficient
# For a given month in a given region, queries the database to calcualte the correlation coefficient
def query_without_threshold(cursor, region, month):
    
    cursor.execute('''SELECT SUM(temperature) FROM deaths_and_temperature WHERE month = ? AND region = ?;''', (month, region))
    x_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(total_deaths) FROM deaths_and_temperature WHERE month = ? AND region = ?;''', (month, region))
    y_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * temperature) FROM deaths_and_temperature WHERE month = ? AND region = ?;''', (month, region))
    x_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(total_deaths * total_deaths) FROM deaths_and_temperature WHERE month = ? AND region = ?;''', (month, region))
    y_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT COUNT(*) FROM deaths_and_temperature WHERE month = ? AND region = ?;''', (month, region))
    count_x = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * total_deaths) FROM deaths_and_temperature WHERE month = ? AND region = ?;''', (month, region))
    sum_xy_product = cursor.fetchone()[0]
   
    # math function for the correlation
    try:
        correlation_coefficient = (count_x * sum_xy_product - (x_sum * y_sum)) / math.sqrt((count_x * x_squared_sum - x_sum * x_sum) * (count_x * y_squared_sum - y_sum * y_sum))
    # if denominator is zero, error occurs
    except ZeroDivisionError:
        print("Error: Division by zero. This could be due to the dataset being too small or no data points meeting the threshold.")
        correlation_coefficient = None
    
    return correlation_coefficient

# Visualize: heatmap with x-axis: Month and y-axis: correlation coefficient
# Creates a heatmap of the correlation coefficients over the months of the year in different regions
def visualization(threshold):
    # Create a DataFrame from the data
    df = pd.DataFrame(calculate_correlation(threshold), columns=['Month', 'Region', 'Correlation coefficient', 'Threshold'])

    # Drop rows with null 'Correlation coefficient' values
    df = df.dropna(subset=['Correlation coefficient'])

    # Visualize relationship between total deaths and temperature across all regions
    sns.lmplot(x='Month', y='Correlation coefficient', hue='Region', data=df)

    plt.title('Heatmap')
    plt.show()

graph_database_node = PythonOperator(
    task_id="visualization",
    dag=query_DAG,
    trigger_rule="all_success",
    # threshold = None, if question is 2 --> make threshold = 32.0°
    python_callable=visualization(None),
)

end_node = EmptyOperator(
    task_id="end_task", 
    dag=query_DAG, 
    trigger_rule="all_success"
)

start_node >> graph_database_node >> correlation_one_node >> correlation_two_node >> end_node