--------------------------------------------
# IN THE RECORDING

$ python --version
# Should be running 

$ airflow version
# 2.9.2

$ airflow scheduler

# Open another tab in the terminal
$ conda activate /Users/loonycorn/anaconda3/envs/airflowenv

$ pip install pandas

$ airflow webserver

# Show already logged in to the web server UI


--------------------------------------------
# Create a folder called source_data/ under airflow/

# airflow/source_data/ should initially be empty

# Create a folder called output_data/ under airflow/

# airflow/output_data/ should initially be empty

# Original dataset
# Dataset: Financial/Wall Street Market Data - Fictional.csv

# Show the directories set up in VS Code

# Show the shares_1.csv file

# data_pipeline_with_branches_v1.py
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
   'owner': 'loonycorn'
}

with DAG(
    dag_id = 'data_pipeline_with_branches',
    description = 'A data pipeline that uses branching operations',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:

    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = '/Users/loonycorn/airflow/source_data/shares_1.csv',
        poke_interval = 10,
        timeout = 60 * 10
    )


    checking_for_file

# Go to the airflow UI

# Go to 

Admin -> Connections

# Search for fs_default and show this (this is what the file sensor uses)

# Default value is File Path (show the drop down and show otehr values)  

# Go to the Grid view

# Unpause and trigger the DAG

# It'll keep waiting for the file to be present

# Click through to the Logs and show it keeps poking and waiting

# Now drag the shares_1.csv file to the source_data/ folder

# Immediately switch over to the UI

# Show the task is complete

# Show the logs

-------------------------------------------------------------------------

# Delete the shares_1.csv file from source_data/

# Open and show all the files inside the dataset/ folder

> Go back to "localhost:8080"

# IMPORTANT: Open up Admin -> Variables in a new tab

# > Admin -> Variables -> Add

Key: action_to_perform
Value: filter
Save

Key: company_to_filter_by
Value: amzn
Save

# data_pipeline_with_branches_v2.py
import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import glob

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


default_args = {
   'owner': 'loonycorn'
}

DATASETS_PATH = '/Users/loonycorn/airflow/source_data/shares_*.csv'
OUTPUT_PATH = '/Users/loonycorn/airflow/output_data/'

def read_csv_files():
    combined_df = pd.DataFrame()
    
    for file in glob.glob(DATASETS_PATH):
        df = pd.read_csv(file)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    return combined_df.to_json(orient='records')

def removing_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_files')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()
    
    return df.to_json()    

def determine_branch():
    action_to_perform = Variable.get("action_to_perform", default_var=None)
    company_to_filter_by = Variable.get("company_to_filter_by", default_var=None)
    
    # Return the task id of the task to execute next
    if action_to_perform == 'filter':
         return f'filter_by_{company_to_filter_by.lower()}'
    elif action_to_perform == 'groupby_ticker':
        return 'groupby_ticker'

def filter_by_ticker(**kwargs):
    ticker = kwargs['ticker']
    json_data = kwargs['ti'].xcom_pull(task_ids='removing_null_values')
    
    df = pd.read_json(json_data)
    
    filtered_df = df[df['Symbol'] == ticker]
    
    output_file = os.path.join(OUTPUT_PATH, f"{ticker.lower()}.csv")
    filtered_df.to_csv(output_file, index=False)

def groupby_ticker(**kwargs):
    json_data = kwargs['ti'].xcom_pull(task_ids='removing_null_values')
    
    df = pd.read_json(json_data)
    
    grouped_df = df.groupby('Symbol').agg({
        'Open': 'mean',
        'High': 'mean',
        'Low': 'mean',
        'Close': 'mean',
        'Volume': 'mean'
    }).reset_index()
    
    output_file = os.path.join(OUTPUT_PATH, "grouped_by_ticker.csv")
    grouped_df.to_csv(output_file, index=False)

with DAG(
    dag_id = 'data_pipeline_with_branches',
    description = 'A data pipeline that uses branching operations',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:

    start = DummyOperator(
        task_id='start'
    )
    
    end = DummyOperator(
        task_id='end'
    )

    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = DATASETS_PATH,
        poke_interval = 10,
        timeout = 60 * 10
    )

    read_csv_files = PythonOperator(
        task_id='read_csv_files',
        python_callable=read_csv_files
    )
    
    removing_null_values = PythonOperator(
        task_id='removing_null_values',
        python_callable=removing_null_values
    )
    
    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    filter_by_aapl = PythonOperator(
        task_id='filter_by_aapl',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'AAPL'}
    )
    
    filter_by_googl = PythonOperator(
        task_id='filter_by_googl',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'GOOGL'}
    )
    
    filter_by_amzn = PythonOperator(
        task_id='filter_by_amzn',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'AMZN'}
    )
    
    groupby_ticker = PythonOperator(
        task_id='groupby_ticker',
        python_callable=groupby_ticker
    )
    
    delete_files = BashOperator(
        task_id='delete_files',
        bash_command='rm -rf /Users/loonycorn/airflow/source_data/*.csv',
        trigger_rule='one_success'
    )


start >> checking_for_file >> read_csv_files >> removing_null_values >> determine_branch

determine_branch >> \
    [filter_by_aapl, filter_by_googl, filter_by_amzn, groupby_ticker] >> \
    delete_files >> end



# Go to the UI and show the DAG

# Show the Grid 

# Go to the Graph

# Unpause and auto-refresh the DAG

# Show that the file sensor is waiting for the files

# Go back to the graph

# Now add shares_1.csv and shares_2.csv to the source_data/ folder

# Go back to the graph and show the DAG executing

# Show the source_data/ folder is empty

# Show the contents in the output_data/ folder

# Open the amzn.csv file and show

# Go to the UI

# Go to Admin -> Variables

# Edit the company_to_filter_by

googl

# Back to the UI - trigger the DAG again

# Now add shares_3, shares_4, shares_5 to the source_data folder

# Go back to the graph and show the DAG executing

# Once execution is complete go to the Finder window

# Show the source_data/ folder is empty

# Show the contents of the files in the output/ folder

# Open the googl.csv file and show


# Go to the UI

# Go to Admin -> Variables

# Change the action_to_perform to groupby_ticker

# Trigger the DAG - it will wait at checking_for_file

# Now add shares_6, shares_7, shares_8 to the source_data/ folder

# Show the DAG is executing

# Once it's done, show the output_data/ 

-------------------------------------------------------------------------

# Create a folder called operators/ under airflow/dags/

# Create a file called "custom_cleaning_operator.py" under airflow/dags/operators/

# custom_cleaning_operator.py
import pandas as pd
import glob
from datetime import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CustomCleaningOperator(BaseOperator):
    @apply_defaults
    def __init__(self, datasets_path, **kwargs):
        super().__init__(**kwargs)
        self.datasets_path = datasets_path

    def execute(self, context):
        ti = context['ti']
        
        combined_df = pd.DataFrame()

        for file in glob.glob(self.datasets_path):
            df = pd.read_csv(file)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        df = combined_df.dropna()

        ti.xcom_push(key='cleaned_data', value=df.to_json())


# In the above code, we have created a custom operator called CustomCleaningOperator that 
# reads all the CSV files in the given path, combines them into a single DataFrame, and then 
# removes any null values. The resulting DataFrame is then pushed to XCom with the key 
# transformed_data.

# Go to the data_pipeline_with_branches.py file and make the following changes

# Add import

from operators.custom_cleaning_operator import CustomCleaningOperator


# Delete this method

def removing_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_files')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()
    
    return df.to_json()    


# In filter_by_ticker replace this

    json_data = kwargs['ti'].xcom_pull(task_ids='removing_null_values')

# with 

    json_data = kwargs['ti'].xcom_pull(key="cleaned_data", task_ids='clean_data')


# In groupby_ticker replace this

    json_data = kwargs['ti'].xcom_pull(task_ids='removing_null_values')

# with 

    json_data = kwargs['ti'].xcom_pull(key="cleaned_data", task_ids='clean_data')

# In the DAG replace this


    removing_null_values = PythonOperator(
        task_id='removing_null_values',
        python_callable=removing_null_values
    )

# with
    
    clean_data = CustomCleaningOperator(
        task_id='clean_data',
        datasets_path=DATASETS_PATH
    )


# In the dependencies bitshift operator replace this

removing_null_values

# with

clean_data


# This is what the code should look like

# data_pipeline_with_branches_v3.py

import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import glob

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

from operators.custom_cleaning_operator import CustomCleaningOperator

default_args = {
   'owner': 'loonycorn'
}

DATASETS_PATH = '/Users/loonycorn/airflow/source_data/shares_*.csv'
OUTPUT_PATH = '/Users/loonycorn/airflow/output_data/'

def read_csv_files():
    combined_df = pd.DataFrame()
    
    for file in glob.glob(DATASETS_PATH):
        df = pd.read_csv(file)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    return combined_df.to_json(orient='records')

def determine_branch():
    action_to_perform = Variable.get("action_to_perform", default_var=None)
    company_to_filter_by = Variable.get("company_to_filter_by", default_var=None)
    
    # Return the task id of the task to execute next
    if action_to_perform == 'filter':
         return f'filter_by_{company_to_filter_by.lower()}'
    elif action_to_perform == 'groupby_ticker':
        return 'groupby_ticker'

def filter_by_ticker(**kwargs):
    ticker = kwargs['ticker']

    json_data = kwargs['ti'].xcom_pull(key="cleaned_data", task_ids='clean_data')
    
    df = pd.read_json(json_data)
    
    filtered_df = df[df['Symbol'] == ticker]
    
    output_file = os.path.join(OUTPUT_PATH, f"{ticker.lower()}.csv")
    filtered_df.to_csv(output_file, index=False)

def groupby_ticker(**kwargs):
    json_data = kwargs['ti'].xcom_pull(key="cleaned_data", task_ids='clean_data')
    
    df = pd.read_json(json_data)
    
    grouped_df = df.groupby('Symbol').agg({
        'Open': 'mean',
        'High': 'mean',
        'Low': 'mean',
        'Close': 'mean',
        'Volume': 'mean'
    }).reset_index()
    
    output_file = os.path.join(OUTPUT_PATH, "grouped_by_ticker.csv")
    grouped_df.to_csv(output_file, index=False)

with DAG(
    dag_id = 'data_pipeline_with_branches',
    description = 'A data pipeline that uses branching operations',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:

    start = DummyOperator(
        task_id='start'
    )
    
    end = DummyOperator(
        task_id='end'
    )

    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = DATASETS_PATH,
        poke_interval = 10,
        timeout = 60 * 10
    )

    read_csv_files = PythonOperator(
        task_id='read_csv_files',
        python_callable=read_csv_files
    )
    
    clean_data = CustomCleaningOperator(
        task_id='clean_data',
        datasets_path=DATASETS_PATH
    )
    
    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    filter_by_aapl = PythonOperator(
        task_id='filter_by_aapl',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'AAPL'}
    )
    
    filter_by_googl = PythonOperator(
        task_id='filter_by_googl',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'GOOGL'}
    )
    
    filter_by_amzn = PythonOperator(
        task_id='filter_by_amzn',
        python_callable=filter_by_ticker,
        op_kwargs={'ticker': 'AMZN'}
    )
    
    groupby_ticker = PythonOperator(
        task_id='groupby_ticker',
        python_callable=groupby_ticker
    )
    
    delete_files = BashOperator(
        task_id='delete_files',
        bash_command='rm -rf /Users/loonycorn/airflow/source_data/*.csv',
        trigger_rule='one_success'
    )


start >> checking_for_file >> read_csv_files >> clean_data >> determine_branch

determine_branch >> \
    [filter_by_aapl, filter_by_googl, filter_by_amzn, groupby_ticker] >> \
    delete_files >> end



# Go to the UI and show the DAG

# Show the Grid

# Go to the Graph

# Unpause and auto-refresh the DAG

# Show that the file sensor is waiting for the files

# Show the logs for the File sensor

# Now add shares_1.csv to shares_8.csv to the source_data/ folder

# Go back to the graph and show the DAG executing

# Show the source_data/ folder is empty

# Show the contents in the output_data/ folder

