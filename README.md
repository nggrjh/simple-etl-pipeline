# Pacmann Data Eng Intro

## Requirements Gathering & Solution

## Designing ETL Pipeline

## ETL Implementation & Scheduling

## Testing Scenario

Make sure you have the following installed on your system:

- Python 3: [Install Python 3](https://realpython.com/installing-python/)
- Docker: [Install Docker](https://docs.docker.com/get-docker/)
- Docker Compose: [Install Docker Compose](https://docs.docker.com/compose/install/)

Before you start the ETL Pipeline, prepare all the dependencies:

- Run your Docker application.
- Prepare the data source in the local PostgreSQL and initiate the new data warehouse database by executing this command:

  ```sh
  docker-compose up -d --build
  ```

- Ensure that the `etl_db` is accessible from `localhost:5432`, and `data_warehouse` is accessible from `localhost:5433`.

- Install the required dependencies by running the following command in your terminal:

  ```sh
  pip install -r requirements.txt
  ```

Once everything is done, to run the ETL pipeline, simply execute the following command:

```sh
python etl.py
```

This command will start the ETL process and populate the data warehouse with the extracted, transformed, and loaded data.
