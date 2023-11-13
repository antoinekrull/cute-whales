# cute-whales
A project for the data engneering class at INSA Lyon.

### Presentation
[Click here to go to the presentation](presentation.md)

## Get started
1. make sure Docker Desktop is running.
2. run `mkdir -p ./dags ./logs ./plugins ./config` og `echo -e "AIRFLOW_UID=$(id -u)" > .env`
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
    t1((1°-5°)) -- Berlin, April --> d1((117))
    t2((6°-10)) -- Berlin, October --> d2((87))
    t3((11°-15°)) -- Paris, April --> d3((14))
    t4((16°-20°)) --Berlin, May --> d4((157))
    t5((21°-25°)) -- Paris, May --> d5((204))
    t6((-5°-0°)) -- Berlin, November --> d6((59))
    t7((-10°- -6°)) -- Berlin, January --> d3((14))
    t8((11°-15°)) -- Paris, October --> d7((143))
```

## Data Pipeline Design

### Ingestion (Pipeline 1):
Ingest city temperature data from sources.
Ingest German and French mortality data from sources.
Store this data in a landing zone, which could be cloud-based storage or a local database.
Use Apache Airflow to automate data ingestion and schedule updates.

### Staging (Pipeline 2):
Clean and preprocess the raw data, addressing missing values or inconsistencies.
Join the cleaned whale movement data with oceanographic data to enrich the dataset.
Transform the data into a structured format suitable for analysis.
Persist the combined data into a staging zone for durability.

### Production Analytics (Pipeline 3):
Design a database to store the cleaned and enriched data.
Implement SQL queries to analyze the data based on the formulated questions.
Create data marts or views to facilitate analytical queries.
Use Apache Airflow for scheduling regular updates of the data marts.

