# Project: Data Warehouse

The goal of this project is to build an ETL pipeline for a database hosted on Amazon Redshift. The pipeline loads data from Amazon S3 into staging tables on Redshift and executes SQL statements to create analytics tables from these staging tables.

## Project Steps

### Step 1: Queries Creation

In this step, we create all required SQL queries in the `sql_queries.py` file. These queries include:

- Creating Fact and Dimension tables
- Loading data from S3 into staging tables
- Inserting data into final tables from staging tables
- Retrieving data ready for analysis

### Step 2: Tables Creation

In this step, we write a Python script called `create_tables.py` to execute table creation queries.

### Step 3: Data Loading

In this step, we write a Python script called `etl.py` to load data from Amazon S3 to staging tables on Redshift and insert loaded data into final tables.

### Step 4: Retrieve Data

In this step, we write a Python script called `analysis.py` to retrieve data from final tables for analysis.

### Step 5: Solution Running

In this step, we write a Jupyter Notebook called `project_exe.ipynb` that helps execute all the previous scripts step by step.

## How to Run the Python Scripts

1. Ensure that all required Python packages are installed (e.g., `psycopg2`, `pandas`, `boto3`).
2. Configure the `dwh.cfg` file with your Amazon Redshift cluster and AWS credentials.
3. Run the `create_tables.py` script to create the necessary tables in Redshift.
4. Run the `etl.py` script to load data from S3 into staging tables and insert it into final tables.
5. Run the `analysis.py` script to retrieve data from final tables for analysis.

## Files in the Repository

- `sql_queries.py`: Contains SQL queries for creating tables, loading data, and inserting data into final tables.
- `create_tables.py`: Python script to create tables in Redshift.
- `etl.py`: Python script to load data from S3 and insert it into final tables.
- `analysis.py`: Python script to retrieve data from final tables for analysis.
- `project_exe.ipynb`: Jupyter Notebook to execute the scripts step by step.
- `dwh.cfg`: Configuration file containing Redshift cluster and AWS credentials.
- `README.md`: Documentation explaining the project, how to run the scripts, and the files in the repository.
