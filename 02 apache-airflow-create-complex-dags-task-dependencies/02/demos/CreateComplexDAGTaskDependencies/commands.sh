-------------------------------------------------------------------------------
# BEHIND THE SCENES

# Start your demo in a virtual env with airflow set up

Virtual Environment name: airflowenv

# Activate a virtual environment behind the scenes
# Make sure that you are running a recent version of Python in the venv (Python 10)

# Pre-create an Admin user and keep
$ airflow users create \
-e cloud.user@loonycorn.com \
-f CloudUser \
-l Loonycorn \
-p password \
-r Admin \
-u loonycorn 

$ airflow users list
# id | username  | email                    | first_name | last_name | roles
# ===+===========+==========================+============+===========+======
# 1  | loonycorn | cloud.user@loonycorn.com | loonycorn  | loonycorn | Admin

# On the terminal window (should be able to see )

$ cd /Users/loonycorn/airflow

$ ls -l

# Open airflow.cfg file 

# Scroll down to "load_examples"

load_examples = False

# Save the file and close it

-------------------------------------------------------------------------------
# IN THE RECORDING

# Within the virtual environment

$ python --version
# Should be running 

$ airflow version
# 2.9.2

$ airflow scheduler

# Open another tab in the terminal
$ conda activate /Users/loonycorn/anaconda3/envs/airflowenv

$ pip install pandas

$ airflow webserver


# Login to the webserver UI - select active graphs, there should be no graphs right now

-------------------------------------------------------------------------------
# Behind the scenes

# Show that there are no active graphs - have the active graphs tab selected

# Now open the "airflow" folder is "vscode" present in "~/airflow"

# Create a folder called "datasets" in "~/airflow/" > Copy "ecommerce_marketing.csv"

# Dataset original:
Essentials /Marketing - recordid_name_gender_age_location_email_phone_product_category_amount.csv


# So we have to create a folder called "dags" in "~/airflow/"

# Once the "dags" folder is created we will be creating all of our DAGs inside this folder

# Create a file called data_processing_pipeline.py

-------------------------------------------------------------------------------
# IN THE RECORDING

# Open and show the ecommerce_marketing.csv file in Numbers (Mac App)

# Note the empty cells with no data

# Show the folder structure that has already been set up on the left

# data_processing_pipeline_v1.py

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'loonycorn',
}

with DAG(
    dag_id='complex_data_pipeline',
    description = 'Data processing pipeline with multiple operators and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    check_file_exists = BashOperator(
        task_id = 'check_file_exists',
        bash_command = 'test -f /Users/loonycorn/airflow/datasets/ecommerce_marketing.csv || exit 1'
    )

check_file_exists

# Now go back to the browser at "localhost:8080"

# Go to All and click on the "complex_data_pipeline" and activate it

# It will now be in Active Graphs

# Now goto the "Graph View" here we see one operator called which is a BashOperator

# Click on Code to see the code for the DAG that we will be executing

# Now get go back to "Grid View" click on Trigger DAG

# It should run through successfully

# Now lets switch to "Graph View" here click on the "check_file_exists" -> click on "Log"

# [2024-06-11, 13:56:40 UTC] {subprocess.py:86} INFO - Output:
# [2024-06-11, 13:56:40 UTC] {subprocess.py:97} INFO - Command exited with return code 0


--------------------------------------------------------------------------

# data_processing_pipeline_v2.py

# Make the changes to add another operator

# Add import (this will fail if pandas is not installed in the env Airflow is runningin)
import pandas as pd

# and below

from airflow.operators.python import PythonOperator

# Add function


def remove_null_values():
    df = pd.read_csv('/Users/loonycorn/airflow/datasets/ecommerce_marketing.csv')
    
    df = df.dropna()

    print(df)
    
    df.to_csv('/Users/loonycorn/airflow/datasets/ecommerce_marketing_cleaned.csv', index=False)


# Add operator

    clean_data = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

# Update DAG invocation

check_file_exists.set_downstream(clean_data)


# The DAG should look like this:
import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'loonycorn',
}

def remove_null_values():
    df = pd.read_csv('/Users/loonycorn/airflow/datasets/ecommerce_marketing.csv')
    
    df = df.dropna()

    print(df)
    
    df.to_csv('/Users/loonycorn/airflow/datasets/ecommerce_marketing_cleaned.csv', index=False)


with DAG(
    dag_id='complex_data_pipeline',
    description = 'Data processing pipeline with multiple operators and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    check_file_exists = BashOperator(
        task_id = 'check_file_exists',
        bash_command = 'test -f /Users/loonycorn/airflow/datasets/ecommerce_marketing.csv || exit 1'
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=remove_null_values
    )


check_file_exists.set_downstream(clean_data)


# Now go back to the browser at "localhost:8080"

# Click on the "complex_data_pipeline"

# Now goto the "Graph View" here we see two operators called which is a BashOperator and PythonOperator

# Click on "Run" to run the DAG

# Open and show the logs for both the operators

# Show the file written out in the datasets/ folder

# Open and show ecommerce_marketing_cleaned.csv in Numbers (show no empty cells)

--------------------------------------------------------------------------

# Let's create a SQLlite database that we can use

> cd ~/airflow/

> mkdir database

> cd database

> touch my_sqlite.db

> sqlite3 my_sqlite.db

> .databases

> .tables

# > Goto "localhost:8080"
# > Click on the "Admin" tab in the top navigation bar.
# > Click on "Connections" in the dropdown menu.
# > Click on the "+" button on the left side of the page to add a new record

Conn Id: my_sqlite_conn
Conn Type: sqlite
Host: /Users/loonycorn/airflow/database/my_sqlite.db

# IMPORTANT: Clear the login and password if it has been auto-populated

# Note: "my_sqlite.db" is a sqlite DB which we just created

# > Click "Test" 
# > Click "Save"

--------------------------------------------------------------------------
# data_processing_pipeline_v3.py

import pandas as pd
import sqlite3

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator

from airflow.hooks.base_hook import BaseHook

ORIGINAL_DATA = '/Users/loonycorn/airflow/datasets/ecommerce_marketing.csv'
CLEANED_DATA = '/Users/loonycorn/airflow/datasets/ecommerce_marketing_cleaned.csv'
AGGREGATED_DATA = '/Users/loonycorn/airflow/datasets/ecommerce_marketing_aggregated.csv'

SQLITE_CONN_ID = 'my_sqlite_conn'

default_args = {
    'owner' : 'loonycorn',
}

def remove_null_values():
    df = pd.read_csv(ORIGINAL_DATA)
    
    df = df.dropna()

    print(df)
    
    df.to_csv(CLEANED_DATA, index=False)

def aggregate_data():
    df = pd.read_csv(CLEANED_DATA)
    
    aggregated_df = df.groupby(['Gender', 'Product', 'Category'])['Amount'].mean().reset_index()
    
    aggregated_df = aggregated_df.sort_values(by='Amount', ascending=False)
    
    print(aggregated_df)
    
    aggregated_df.to_csv(AGGREGATED_DATA, index=False)

def insert_into_sqlite():
    connection = BaseHook.get_connection(SQLITE_CONN_ID)

    conn = sqlite3.connect(connection.host)

    cursor = conn.cursor()

    with open(AGGREGATED_DATA, 'r') as f:
        # Skip header
        next(f)

        for line in f:
            gender, product, category, avg_amount = line.strip().split(',')
            
            cursor.execute(
                "INSERT INTO aggregated_ecommerce_data (Gender, Product, Category, AvgAmount) VALUES (?, ?, ?, ?)",
                (gender, product, category, avg_amount)
            )
    
    conn.commit()
    conn.close()


with DAG(
    dag_id='complex_data_pipeline',
    description = 'Data processing pipeline with multiple operators and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    check_file_exists = BashOperator(
        task_id = 'check_file_exists',
        bash_command = f'test -f {ORIGINAL_DATA} || exit 1'
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=remove_null_values
    )


    aggregate_data = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data
    )

    drop_table_if_exists = SqliteOperator(
        task_id='drop_table_if_exists',
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS aggregated_ecommerce_data;
        """
    )

    create_table = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
            CREATE TABLE aggregated_ecommerce_data (
                Gender TEXT,
                Product TEXT,
                Category TEXT,
                AvgAmount FLOAT
            )
        """
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=insert_into_sqlite
    )


check_file_exists.set_downstream(clean_data)

clean_data.set_downstream(aggregate_data)

aggregate_data.set_downstream(drop_table_if_exists)

create_table.set_upstream(drop_table_if_exists)

load_data.set_upstream(create_table)

# Now go back to the browser at "localhost:8080"

# Click on the "complex_data_pipeline"

# Now goto the "Graph View" here we see five operators called which is a BashOperator, PythonOperator and SqliteOperator

# Click on "Run" to run the DAG (will run through successfully)

# Show the datasets/ folder and the files created there

# Open and show the "ecommerce_marketing_aggregated.csv" in Numbers (Mac App)

# Show the Logs and the create table has run through

# > Go back to the terminal and run these commands

> .tables

> .schema aggregated_ecommerce_data;

> SELECT * FROM aggregated_ecommerce_data;

# Replace the downstreams with bitshift operators

check_file_exists >> clean_data >> aggregate_data >> drop_table_if_exists >> create_table >> load_data

# Now go back to the browser at "localhost:8080"

# Run the DAG - just show that it runs through (nothing else)

-------------------------------------------------------------------------------
# Now lets make use of XComs (this is usually used for small amounts of data)

# In the finder window show that you have deleted everything in the datasets/ folder

# Click on "Admin" > "XComs"

# you should find the "return_value" XCom for the BashOperator

# Click on the "taskId" it should take you to the graph

# Click on the BashOperator and click on XCom -> You can see that the return value returns nothing


# data_processing_pipeline_v4.py

import pandas as pd
import sqlite3
import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator

from airflow.hooks.base_hook import BaseHook

ORIGINAL_DATA = '/Users/loonycorn/airflow/datasets/ecommerce_marketing.csv'

SQLITE_CONN_ID = 'my_sqlite_conn'

default_args = {
    'owner' : 'loonycorn',
}

def remove_null_values(**kwargs):
    df = pd.read_csv(ORIGINAL_DATA)
    
    df = df.dropna()

    ti = kwargs['ti']

    cleaned_data_dict = df.to_dict(orient='records')
    cleaned_data_json = json.dumps(cleaned_data_dict)

    ti.xcom_push(key='cleaned_data', value=cleaned_data_json)

def aggregate_data(**kwargs):
    ti = kwargs['ti']

    cleaned_data_json = ti.xcom_pull(task_ids='clean_data', key='cleaned_data')
    cleaned_data_dict = json.loads(cleaned_data_json)

    df = pd.DataFrame(cleaned_data_dict)
    
    aggregated_df = df.groupby(['Gender', 'Product', 'Category'])['Amount'].mean().reset_index()
    
    aggregated_df = aggregated_df.sort_values(by='Amount', ascending=False)
    
    aggregated_data_dict = aggregated_df.to_dict(orient='records')
    aggregated_data_json = json.dumps(aggregated_data_dict)

    ti.xcom_push(key='aggregated_data', value=aggregated_data_json)

def insert_into_sqlite(**kwargs):
    ti = kwargs['ti']

    aggregated_data_json = ti.xcom_pull(task_ids='aggregate_data', key='aggregated_data')
    aggregated_data_dict = json.loads(aggregated_data_json)

    connection = BaseHook.get_connection(SQLITE_CONN_ID)

    conn = sqlite3.connect(connection.host)

    cursor = conn.cursor()

    for row in aggregated_data_dict:
        cursor.execute(
            "INSERT INTO aggregated_ecommerce_data (Gender, Product, Category, AvgAmount) VALUES (?, ?, ?, ?)",
            (row['Gender'], row['Product'], row['Category'], row['Amount'])
        )
    
    conn.commit()
    conn.close()


with DAG(
    dag_id='complex_data_pipeline',
    description = 'Data processing pipeline with multiple operators and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    check_file_exists = BashOperator(
        task_id = 'check_file_exists',
        bash_command = f'test -f {ORIGINAL_DATA} || exit 1'
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=remove_null_values
    )


    aggregate_data = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data
    )

    drop_table_if_exists = SqliteOperator(
        task_id='drop_table_if_exists',
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS aggregated_ecommerce_data;
        """
    )

    create_table = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id=SQLITE_CONN_ID,
        sql="""
            CREATE TABLE aggregated_ecommerce_data (
                Gender TEXT,
                Product TEXT,
                Category TEXT,
                AvgAmount FLOAT
            )
        """
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=insert_into_sqlite
    )

check_file_exists >> clean_data >> aggregate_data >> drop_table_if_exists >> create_table >> load_data



# Now go back to the browser at "localhost:8080"

# Click on "Run" to run the DAG

# Open and show the logs and Xcoms for 1-2 tasks (clean_data, aggregated_data)

# Click on "Admin" > "XComs"
# (We see all the XComs here)

















