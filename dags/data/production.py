import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import math
import psycopg2

# Return: List
# Iterates over all the regions in the dataset and all the months of a year and returns a list with the correlation coefficient for 
# each region and month
def calculate_correlation(threshold):
    # Define PostgreSQL connection parameters
    postgres_params = {
        "host": "127.0.0.1",
        "port": 5439,
        "database": "postgres",
        "user": "test",
        "password": "test",
    }

    # Connect to the postgres databse
    connection = psycopg2.connect(**postgres_params)
    cursor = connection.cursor()

    cursor.execute('''SELECT DISTINCT region FROM deaths_and_temperature;''')
    regions = cursor.fetchall()

    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    liste = []
    for region in regions:
        region = region[0]

        for month in months:
            if threshold == None:
                liste.append({'Month': month, 'Region': region, 'Correlation coefficient': query_without_threshold(cursor, region, month), 'Threshold': None})
            else:
                liste.append({'Month': month, 'Region': region, 'Correlation coefficient': query_with_threshold(cursor, region, month, threshold), 'Treshold': threshold})

    # Commit and close the connection
    connection.commit()
    cursor.close()
    connection.close()

    return liste

# Return: correlation coefficient
# For a given month in a given region, queries the database to calcualte the correlation coefficient for a spesific threshold for the temperature
def query_with_threshold(cursor, region, month, threshold):
    
    cursor.execute('''SELECT SUM(temperature) FROM deaths_and_temperature WHERE month = %s AND region = %s AND temperature >= %s;''', (month, region, threshold))
    x_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(totaldeaths) FROM deaths_and_temperature WHERE month = %s AND region = %s AND temperature >= %s;''', (month, region, threshold))
    y_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * temperature) FROM deaths_and_temperature WHERE month = %s AND region = %s AND temperature >= %s;''', (month, region, threshold))
    x_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(totaldeaths * totaldeaths) FROM deaths_and_temperature WHERE month = %s AND region = %s AND temperature >= %s;''', (month, region, threshold))
    y_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT COUNT(*) FROM deaths_and_temperature WHERE month = %s AND region = %s AND temperature >= %s;''', (month, region, threshold))
    count_x = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * totaldeaths) FROM deaths_and_temperature WHERE month = %s AND region = %s AND temperature >= %s;''', (month, region, threshold))
    sum_xy_product = cursor.fetchone()[0]
   
    # math function for the correlation
    try:
        correlation_coefficient = (count_x * sum_xy_product - (x_sum * y_sum)) / math.sqrt((count_x * x_squared_sum - x_sum * x_sum) * (count_x * y_squared_sum - y_sum * y_sum))
    # if denominator is zero, error occurs
    except ZeroDivisionError:
        print("Error: Division by zero. This could be due to the dataset being too small or no data points meeting the threshold.")
        correlation_coefficient = None

    return correlation_coefficient

# Return: correlation coefficient
# For a given month in a given region, queries the database to calcualte the correlation coefficient
def query_without_threshold(cursor, region, month):
    
    cursor.execute('''SELECT SUM(temperature) FROM deaths_and_temperature WHERE month = %s AND region = %s;''', (month, region))
    x_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(totaldeaths) FROM deaths_and_temperature WHERE month = %s AND region = %s;''', (month, region))
    y_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * temperature) FROM deaths_and_temperature WHERE month = %s AND region = %s;''', (month, region))
    x_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(totaldeaths * totaldeaths) FROM deaths_and_temperature WHERE month = %s AND region = %s;''', (month, region))
    y_squared_sum = cursor.fetchone()[0]

    cursor.execute('''SELECT COUNT(*) FROM deaths_and_temperature WHERE month = %s AND region = %s;''', (month, region))
    count_x = cursor.fetchone()[0]

    cursor.execute('''SELECT SUM(temperature * totaldeaths) FROM deaths_and_temperature WHERE month = %s AND region = %s;''', (month, region))
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

    # Convert 'Month' column to numeric
    df['Month'] = pd.to_numeric(df['Month'])

    # Visualize relationship between total deaths and temperature across all regions
    sns.lmplot(x='Month', y='Correlation coefficient', hue='Region', data=df)

    plt.title('Heatmap')
    plt.show()


if __name__ == "__main__":
    visualization(None)