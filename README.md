# cute-whales
A small project for the data engneering class at INSA Lyon

## Get started
1. use `id -u` to get your computer's user id and update the `AIRFLOW_UID` in the `.env` to match the id.
2. Run `docker-compose up airflow-init` to create a new Docker container, install Airflow and run the airflow initdb command to initialize the Airflow database.
3. Build and run the environment using the `docker-compose up` command.
4. Connect to the airflow dashboard [localhost:8080](http://localhost:8080/), where user and password is `airflow`
5. Add a connection to the postgres SQL database. Navigate To the Admin -> Connections menu, then click the blue + button to add a new connection.
After it is up, add a new connection:

Name - postgres_default
Conn type - postgres
Host - localhost
Port - 5432
Database - airflow
Username - airflow
Password - airflow