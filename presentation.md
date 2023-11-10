---
marp: true
---

# Title: project name (cute whales (?))
Cute Whales is a Python package for simulating whale migration using artificial intelligence techniques. The project is a collaborative effort between the Center for Marine Science and Technology (CMST) and the Machine Intelligence and Computing Research Lab (MICRL).

---

## Project objectives

- Question 1: Are there correlations between temperature variations in major cities (Berlin & Paris) around the world and mortality rates in different regions of Germany and France?

- Question 2: How do temperature-related factors, such as extreme heat events or prolonged cold spells, impact mortality rates in specific regions, and can we identify vulnerable regions?

---

## Our pipeline

figure here
- I have started working on this (S)

---

## Our data

- .csv-file with death in Germany
- .txt-file with death in France
- .json-file with temperature from different regions

---

## Data ingestion (collection)
- show DAG for data ingestion (?)
- show how we store our data in mongoDB (?)
include a photo of our collections (?)
---

## Data staging (wrangling)
- show DAG for data wrangling (?)
- show how we merge out data (?)
- show how we clean and transform the data (?)
which colloms do we remove, what data do we transform and how
- show how we save the clean data to postgres (?)
include a stardiagram of our postgres-database (?)
---

## Data production 
- how do we want to visualize our data?
- show DAG for visualizing the data (?)
- what is our query to access the data needed to answer our questions?

---

# Thank you (?)