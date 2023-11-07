# cute-whales
A small project for the data engneering class at INSA Lyon

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