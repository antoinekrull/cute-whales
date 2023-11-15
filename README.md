# cute-whales
A project for the data engneering class at INSA Lyon.

### Presentation
[Click here to go to the presentation](presentation.md)

## Get started
1. make sure Docker Desktop is running.
2. run `mkdir -p ./dags ./logs ./plugins ./config` and `echo -e "AIRFLOW_UID=$(id -u)" > .env`
3. Run `docker-compose up airflow-init` run database migrations and create the first user account.
4. Build and run the environment using the `docker-compose up` command.
5. Connect to the airflow dashboard [localhost:8080](http://localhost:8080/), where user and password is `airflow`
6. Add a connection to the postgres SQL database. Navigate To the Admin -> Connections menu, then click the blue + button to add a new connection.
After it is up, add a new connection:

After it is up, add a new connection:

* Name - postgres_default
* Conn type - postgres
* Host - localhost
* Port - 5432
* Database - airflow
* Username - airflow
* Password - airflow


# Project presentation
There is said that there can be found a connection between temperature and death. Through our pipeline 
we will try to both find and visualize a link by ingesting three different datasets with, wrangle them to a preferred format and visualize the data through a graph database in order to answer the following questions: 

### Questions:

Question 1: Are there correlations between temperature variations in major cities around the world and mortality rates in different regions of Germany and France?

Question 2: How do temperature-related factors, such as extreme heat events or prolonged cold spells, impact mortality rates in specific regions, and can we identify vulnerable regions?

Our data will be structured something like this, with an example:
| Year | Month | Number of deaths | Region | Temperature | 
| -------- | -------- | -------- | -------- | -------- |
| value  | value   | value  | value   | value  |
| 2020  | July   | 204   | Paris   | 40 (celsius)   |
| 2018  | September   | 178   | Berlin   | 19 (celsius)   |

The vislualization will :
```mermaid
flowchart LR
    t1((1°- 5°)) -- Berlin, April, 2020 --> d1((117))
    t1((1°- 5°)) -- Paris, Mars, 2018 --> d2((57))
    t2((6°-10)) -- Berlin, October, 2015 --> d3((87))
    t3((11°-15°)) -- Paris, April, 2021 --> d1((117))
    t4((16°-20°)) --Berlin, May, 2018 --> d4((158))
    t5((21°-25°)) -- Paris, May, 2019 --> d4((158)) 
    t3((11°-15°)) -- Paris, October, 2016 --> d3((87))
    t4((16°-20°)) -- Berlin, June, 2017 --> d2((57))
```

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
- figure here

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

### Production Analytics (Pipeline 3):
For the production phase we start by saving the data from our postgres-table to neo4j in order to simplify the data as well as query the graph-database in order to answer our questions. In the graph databse the nodes will be City nodes and Temperature-nodes, where the relation between the nodes are the number of deaths in a given month of a given year corresponding to the temperature of this month in this year and the region this temperature was recorded. The databse look like this:
- Include image of grapha-database from neo4j (?)

#### Queries
Query question 1:
MATCH (city:City)-[r:HAS_DEATHS]->(temperature:Temperature)
RETURN city.name, temperature.value, r.year, r.month, r.number_of_deaths, r.region
ORDER BY r.number_of_deaths DESC;

Query question 2:
MATCH (temperature:Temperature)<-[r:HAS_DEATHS]-(city:City)
WHERE r.number_of_deaths > threshold // specify the threshold for extreme events
RETURN city.name, temperature.value, r.year, r.month, r.number_of_deaths, r.region
ORDER BY r.number_of_deaths DESC;

TODO: Find some way to visualize the data from the queries. Mabye implement another task in the production-DAG to ask the queries as well as visualizing the result.