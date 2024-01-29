# cute-whales
A project for the data engneering class at INSA Lyon.

## Get started
1. make sure Docker Desktop is running.
2. run `mkdir -p ./dags ./logs ./plugins ./config` and `echo -e "AIRFLOW_UID=$(id -u)" > .env`
3. Run `docker-compose up airflow-init` run database migrations and create the first user account.
4. Build and run the environment using the `docker-compose up` command.
5. In a new terminal, run `docker ps` to get the mongo container id and copy this.
6. In the dag.py file on line 29, paste your mongo container id.
7. Connect to the airflow dashboard [localhost:8080](http://localhost:8080/), where user and password is `airflow`
8. Add a connection to the postgres SQL database. Navigate To the Admin -> Connections menu, then click the blue + button to add a new connection.

After it is up, add a new connection:

* Name - postgres_db
* Conn type - postgres
* Host - db
* Port - 5432
* Database - postgres
* Username - test
* Password - test

### Get startet; production
If you have the jupyter notebook installed, open a new terminal and nativage to the right repository and run `jupyter notebook` to get access to the production.ipynb-file. You can use `pip install notebook` to install jupyter notebook. Or you can run the production.py file in the directory if you can't get access to the jupyter-file for some reason. 

### Code comment
There has throughtout the prject been some issues with the access to the docker container for different reasons. The method: `get_mongo_container_id():`was written to get the container id for the mongo container, somethimes it works and other times it doesn't. Therefore the mongo container-id is currently set globaly in order to make the pipeline run. 


# Project presentation
There is said that there can be found a connection between temperature and death. Through our pipeline 
we will try to both find and visualize a link by ingesting three different datasets with, wrangle them to a preferred format and visualize the data through a graph database in order to answer the following questions: 

### Questions:

Question 1: Are there correlations between temperature variations in major cities around the world and mortality rates in different regions of Germany and France?

Question 2: How do temperature-related factors, such as extreme heat events or prolonged cold spells, impact mortality rates in specific regions, and can we identify vulnerable regions?

Our data will be structured something like this, with an example:
| Year | Month | Region | Number of deaths | Temperature | 
| -------- | -------- | -------- | -------- | -------- |
| value  | value   | value  | value   | value  |
| 2020  | July   |  Paris  |  204   | 40 (celsius)   |
| 2018  | September   |  Berlin  |  178  | 19 (celsius)   |

## Data sources
The project utilises three different datasources:

### Temperature 
This dataset contains information about XXX in a .json-file an is structured with coloums XXX
- Include visualization of the data (?)

### Deaths in France
This dataset contains information about XXX in a .txt-file an is structured with coloums XXX
- Include visualization of the data (?)

### Deaths in Berlin
This dataset contains information about XXX in a .csv-file an is structured with coloums XXX
- Include visualization of the data (?)

## Data Pipeline Design
![alt text](/Images/Pipeline.png)

### Ingestion (Pipeline 1):
Ingest city temperature data from sources.
Ingest German and French mortality data from sources.
Store this data in a landing zone, which could be cloud-based storage or a local database.
Use Apache Airflow to automate data ingestion and schedule updates.

- Explain how we ingest the data
- Inclue an image of the collections in MongoDB
- Include image of DAG (?)

### Staging (Pipeline 2):
Clean and preprocess the raw data, addressing missing values or inconsistencies.
Join the cleaned whale movement data with oceanographic data to enrich the dataset.
Transform the data into a structured format suitable for analysis.
Persist the combined data into a staging zone for durability.

- Include image of DAG (?)
- Include image of postgres database and how data is saved in table
- Include a STAR-diagram of the postgres

### Production Analytics (Pipeline 3):
For the production phase of the data pipeline we both visualize the data in the postgres-database, as well as query the database in order to caluculate the correlation coeficient between the temperature and the total deaths in a region, for every month. The visualization part is done by creating a a heatmap using seaborn, which is a Python data visualization library based on matplotlib. The x-axis contains the all the months in the dataset and the y-axis visualizes the correlation coefficient. 

#### Queries
To look at the correlation coefficient between the temperature and the total deaths in a region and given month we query the postgres-database. The formula represents the correlation coefficient, denoted as r, which is a measure of the strength and direction of the relationship between two variables:

$$r=\frac{n\sum xy - \sum x \sum y}{\sqrt{((n\sum x^2) - (\sum x)^2) * ((n\sum y^2) - (\sum y)^2)}}$$

n → number of observations, x and y → temperature-variable and death-variable, Σ → summation of a series.
The value of r ranges from -1 to 1. A value of -1 indicates a perfect negative relationship, a value of 1 indicates a perfect positive relationship, and a value of 0 indicates no relationship.

Question 1: \
This task calculates the correlation coefficient between temperature and total deaths for a specified month and region. The code retrieves user-defined variables for month and region, performs SQL queries on the PostgreSQL database, and computes the correlation using a mathematical formula. The output of the query will be on this form:

`[{'Month': month, 'Region': region, 'Correlation coefficient': correlation_coefficient, 'Threshold': "None"}]`

Table: question 1
![alt text](/Images/table_q1.png)

Question 2: \
Similar to the previous task, this calculates the correlation coefficient, but with an additional condition based on a temperature threshold. It considers only data points where the temperature is greater than or equal to the specified threshold. The threshold can vary, but for this example the value will be 20°. The output of the query will be on this form:

`[{'Month': month, 'Region': region, 'Correlation coefficient': correlation_coefficient, 'Threshold': 20.0}]`

Table: question 1
![alt text](/Images/table_q2.png)


#### Data visualization
The visualization is done by creating a heatmap. The create_heatmap function is designed to visualize the correlation coefficients between total deaths and temperature across different months and regions. Values close to 1 or -1 indicate strong correlations, while values close to 0 suggest a weaker correlation.

The calculate_correlation function is called with a specified threshold, and the output is converted into a DataFrame named df. The DataFrame columns are named 'Month', 'Region', 'Correlation coefficient', and 'Threshold'.
Data Cleaning: Rows with null values in the 'Correlation coefficient' column are dropped to ensure a clean dataset.
The Seaborn library (sns) is used to create a scatter plot with a linear fit for the relationship between 'Month' and 'Correlation coefficient'. The x-axis represents the 'Month', and the y-axis represents the 'Correlation coefficient'.

The resulting visualization provides a quick overview of how the correlation coefficients vary across different months and regions. Negative values indicate a negative correlation, while positive values indicate a positive correlation. The strength of the correlation is determined by the magnitude of the coefficient.

Heatmap: question 1
![alt text](/Images/heatmap_q1.png)

As we can see throught the heatmap for the first question, most of the values for both of the regions is close to zero, and this suggests that there is a weaker correlation between temperature and deaths for the different months of the year.

Heatmap: question 2
![alt text](/Images/heatmap_q2.png)

As we can see throught the heatmap for the second question, there aren't that many values indication that most of the months does not have an average value higher that 20 degrees celsius. From the values we have, we can see that all of them are closer to 0, then to -1 or 1, suggesting that there is a weak correlation between temperatures above 20 degrees and deaths for the different months of the year. 

### Future work
For futire work there are several posibilities for the pipeline. We can add more types of weather data in order to answer the second question. In addition to temaerature data, we can add windspeed-data with a threshold for extreme wind, or precipitation data with a threshold for extreme precipitation, etc. We also have the possibility to add death numbers for more regions than just Paris and Berlin. At the same time we can also add the temperature-column for the regions added by cleaning the datasett differently. 
