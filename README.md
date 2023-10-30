# cute-whales
A project for the data engneering class at INSA Lyon


## TODO
- [ ] getting the data at out appartment because of the bad speed of the internet on the library
- [ ] atm data is in the repo in fileformat, think of writing code to download it in the ingestion process to keep the repo clean

### A
- [x] download the temparature dataset
- [ ] check wrangling needs
- [ ] check size

### D

### S

## Questions
Question 1: How do variations in water temperature and ocean currents affect the migratory patterns of different whale species?

Question 2: Can we identify regions where whales are more likely to aggregate based on water temperature and current data?

Question 3: Is there a correlation between specific oceanographic factors and the timing of whale migrations or their choice of feeding areas?

## Data Pipeline Design

### Ingestion (Pipeline 1):
Ingest raw whale movement data from sources like research institutions or databases.
Ingest raw oceanographic data from sources like oceanographic agencies or databases.
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
