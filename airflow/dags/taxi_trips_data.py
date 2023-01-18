import os
from datetime import datetime, timedelta
import pandas as pd
import pyarrow

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import get_current_context
from airflow.contrib.hooks.bigquery_hook import BigQueryHook




download_trips_file_path = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
trips_file_name =  'yellow_tripdata_{{(logical_date - macros.timedelta(days=150)).strftime(\'%Y-%m\')}}.parquet'
local_data_path = '/opt/airflow/taxi'
BUCKET = 'taxi_data_lake'
PROJECT_ID = 'tribal-logic-361320'

def bq_check_query ():
    context = get_current_context()
    execution_month  = (context["logical_date"] + timedelta(days = -150)).strftime("%m")
    execution_year  = (context["logical_date"] + timedelta(days = -150)).strftime("%Y")

    
    hook = BigQueryHook(gcp_conn_id='gcp_connection', location='europe-central2', delegate_to=None, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT 
            CASE WHEN file > 0 THEN True ELSE False END 
        FROM 
            (SELECT
                COUNT(CASE WHEN filename = 'yellow_tripdata_{execution_year}-{execution_month}.parquet' THEN 1 END) file
            FROM
                `tribal-logic-361320.taxi_data.raw_data`)''')
    result = cursor.fetchone()


    if result[0] == '[False]':
        return 'TripsDataFromGcsToBq'
    else:
        return ['DeleteBqTripsRows', 'TripsDataFromGcsToBq']


def add_filename():
    context = get_current_context()
    execution_month  = (context["logical_date"] + timedelta(days = -150)).strftime("%m")
    execution_year  = (context["logical_date"] + timedelta(days = -150)).strftime("%Y")
    file = f'yellow_tripdata_{execution_year}-{execution_month}.parquet'

    df = pd.read_parquet(f'/opt/airflow/taxi/{file}', engine='pyarrow')
    df['filename'] = file
    df.to_parquet(f'/opt/airflow/taxi/{file}', engine='pyarrow')

def bq_delete_trips_data ():
    context = get_current_context()
    execution_month  = (context["logical_date"] + timedelta(days = -150)).strftime("%m")
    execution_year  = (context["logical_date"] + timedelta(days = -150)).strftime("%Y")
    file = f'yellow_tripdata_{execution_year}-{execution_month}.parquet'
    
    hook = BigQueryHook(gcp_conn_id='gcp_connection', location='europe-central2', delegate_to=None, use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'''
        DELETE FROM `{PROJECT_ID}.taxi_data.raw_data` 
        WHERE filename = '{file}'
        ''')
    

def get_xcom(ti):
    return ti.xcom_pull(key='isintable', task_ids=['table_check'])
    
    

default_args = {
    'owner': 'gorskibartek',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'Taxi_trips_data',
    default_args = default_args,
    start_date = datetime(2022,6,30),
    schedule_interval = '@monthly',
    catchup = True
) as dag:
    download_trips_data = BashOperator(
        task_id = 'DownloadTripsData',
        bash_command = f'wget {download_trips_file_path}{trips_file_name} -O {local_data_path}/{trips_file_name}'
    )
    
    add_trips_file_name_column = PythonOperator(
        task_id = 'AddFilename',
        provide_context = True,
        python_callable = add_filename
    )

    upload_trips_file_to_gcs = LocalFilesystemToGCSOperator(
        gcp_conn_id = 'gcp_connection',
        task_id = 'TripsDataToGcs',
        src = f'{local_data_path}/{trips_file_name}',
        dst = f'yellow_trips/{trips_file_name}',
        bucket = f'{BUCKET}'
    )

    trips_data_from_gcs_to_bq = GCSToBigQueryOperator(
            gcp_conn_id = 'gcp_connection',
            task_id = 'TripsDataFromGcsToBq',
            bucket = BUCKET,
            source_objects = [f'yellow_trips/{trips_file_name}'],
            source_format = 'PARQUET',
            destination_project_dataset_table = f'{PROJECT_ID}.taxi_data.raw_data',
            skip_leading_rows = 1,
            autodetect = True,
            write_disposition = 'WRITE_APPEND',
            create_disposition = 'CREATE_IF_NEEDED',
            allow_jagged_rows = True, #allows for missing values,
    )

    

    trips_table_check = BranchPythonOperator(
    task_id = 'TripsTableCheck',
    provide_context = True,
    python_callable = bq_check_query,
    )



    delete_bq_trips_rows = PythonOperator(
        task_id = 'DeleteBqTripsRows',
        python_callable = bq_delete_trips_data,
        provide_context = True
    )

    #Zones part
    download_zones_file = BashOperator(
        task_id = 'DownloadZonesData',
        bash_command = f'wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv -O {local_data_path}/zones.csv'
    )
    
    upload_zones_file_to_gcs = LocalFilesystemToGCSOperator(
        gcp_conn_id = 'gcp_connection',
        task_id = 'ZonesDataToGcs',
        src = f'{local_data_path}/zones.csv',
        dst = f'zones.csv',
        bucket = f'{BUCKET}'
    )

    zones_data_from_gcs_to_bq = GCSToBigQueryOperator(
            gcp_conn_id = 'gcp_connection',
            task_id = 'ZonesDataFromGcsToBq',
            bucket = BUCKET,
            source_objects = [f'zones.csv'],
            source_format = 'CSV',
            destination_project_dataset_table = f'{PROJECT_ID}.taxi_data.zones_data',
            skip_leading_rows = 1,
            autodetect = True,
            write_disposition = 'WRITE_TRUNCATE',
            create_disposition = 'CREATE_IF_NEEDED',
            allow_jagged_rows = True, #allows for missing values,

    )

    #DBT PART
    dbt_run = BashOperator(
    task_id='DbtRun',
    bash_command='cd /opt/dbt/taxi_data && dbt deps && dbt build --profiles-dir .',
    )

    dbt_test = BashOperator(
    task_id='DbtTest',
    bash_command='cd /opt/dbt/taxi_data && dbt deps && dbt test --profiles-dir .',
    )

    #TRIPS PART
    download_trips_data >> add_trips_file_name_column >> upload_trips_file_to_gcs >>  trips_table_check 
    trips_table_check >> delete_bq_trips_rows
    delete_bq_trips_rows >> trips_data_from_gcs_to_bq
    trips_table_check >> trips_data_from_gcs_to_bq
    
    #ZONES PART
    download_zones_file >> upload_zones_file_to_gcs >> zones_data_from_gcs_to_bq
    
    #DBT PART
    zones_data_from_gcs_to_bq >> dbt_run
    trips_data_from_gcs_to_bq >> dbt_run
    
    dbt_run >> dbt_test
 
