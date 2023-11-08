# cute-whales
A project for the data engneering class at INSA Lyon


## TODO
- [ ] getting the data at out appartment because of the bad speed of the internet on the library
- [ ] atm data is in the repo in fileformat, think of writing code to download it in the ingestion process to keep the repo clean

### A
- [ ] Ingest temperature data & Germany data
- [ ] code to clean .csv files and writes it into the databases

### D
- [ ] Ingest data for France

### S
- [ ] set up mongodb docker container to copy data

## Questions
Question 1: Are there correlations between temperature variations in major cities around the world and mortality rates in different regions of Germany and France?

Question 2: How do temperature-related factors, such as extreme heat events or prolonged cold spells, impact mortality rates in specific regions, and can we identify vulnerable regions?

Question 3: Can climate and temperature data be used to provide early warnings or insights for public health interventions in regions with increased mortality risks due to temperature extremes?

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