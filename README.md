# Simple ETL Pipeline

## Requirements Gathering & Solution

### Problems

- Tim Sales has sales data stored in PostgreSQL Database. However, there are still many missing data, and the data format is incorrect.
- Tim Product also has data on Electronic Product Pricing in CSV format, but the data structure is messy, and there are many missing values.

### Solutions

- The Data Engineer will build a pipeline to extract data from the database, the CSV file, and web scraping sources. All missing values will be addressed, duplicate records will be removed, and the correct data types will be assigned.
- The Data Engineer will create a schedule to ensure that the pipeline is always up-to-date based on the latest data.

## Designing ETL Pipeline

![ETL Pipeline](etl-pipeline.png)

## ETL Implementation

### Extract

- Obtain data from various sources.
- Establish a database connection to retrieve data.
- Read CSV files for data.
- Scrape data from web sources.

### Transform

- Select all required columns.
- Retain one record for each duplicate entry.
- Populate empty fields and ensure consistent data types.
- Convert currency to float for easier manipulation.

### Load

- Store all transformed data into their respective tables.

## ETL Scheduling

Follow these steps to schedule the ETL process in crontab:

- Use `crontab -e` to open crontab.
- Add schedule using this syntax:

  ```sh
  * * * * * python <project_path>/etl.py
  ```

- Check with `crontab -l` to verify.

### Local Environment Setup

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

## Testing Scenario

**Objective**: Validate data results of the ETL pipeline.

**Setup**:

- Each table that represents each data source has been configured having unique identifiers.
- Data warehouse set up to only accept new data.

**Expected Outcome**:

- Newly extracted data should be successfully loaded into the warehouse.
- Existing data should remain unchanged.
