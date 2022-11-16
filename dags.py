import os
import logging
import requests
import glob
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import sqlalchemy
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import snowflake
from constants import *

#########################################################
#
#   Load Environment Variables
#
#########################################################
# Connection variables
SNOWFLAKE_CONN_ID = "snowflake_conn_id"

DAG_ID = "airflow_snowflake_big_data"
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'AT3'

#########################################################
#
#   DAG Function
#
#########################################################
def get_all_csv_file_path_from_local():
    result = glob.glob('/home/airflow/gcs/dags/data/*/**.csv')
    result = [s.replace('\\','/') for s in result]
    result1 = glob.glob('/home/airflow/gcs/dags/data/*.csv')
    result1 = [s.replace('\\','/') for s in result1]
    return result+result1
def Load_Raw_Data():
    con = snowflake.connector.connect(
        account=os.getenv('ACCOUNT'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        role=os.getenv('ROLE'),
        warehouse=os.getenv('WAREHOUSE'),
        database=os.getenv('DATABASE'),
        schema=os.getenv('SCHEMA'),
    )
    #creating file formate for snowflake
    create_file_format= """
                create or replace file format csv_format
                type = csv
                field_delimiter = ','
                skip_header = 1
                null_if = ('NULL', 'null')
                empty_field_as_null = true
                compression = gzip;
                """
    con.cursor().execute(create_file_format)
    for file_path in get_all_csv_file_path_from_local():
        table_name = file_path.split('/')[-1].split('.')[0]
        data = pd.read_csv(file_path)
        dataTypeDict = dict(data.dtypes)
        craete_table = "CREATE TABLE IF NOT EXISTS "+table_name+' ('
        for key in dataTypeDict:
            craete_table += '"'+key+'"' + ' ' +str(dataTypeDict[key])+','
        craete_table = craete_table[:-1]
        craete_table += ');'   
        craete_table = craete_table.replace("object","text")
        craete_table = craete_table.replace("float64","float")
        craete_table = craete_table.replace("int64","BIGINT")
        con.cursor().execute(craete_table)
        con.cursor().execute(f"truncate table {table_name}")
        con.cursor().execute(f"put file://{file_path} @AT3.RAW.%{table_name}")
        con.cursor().execute(f"copy into {table_name} from @%{table_name}  ON_ERROR='CONTINUE' file_format = (format_name = 'csv_format' , error_on_column_count_mismatch=false)") 
    con.close()

def Copy_Data_From_RAW_To_Stage():
    con = snowflake.connector.connect(
        account=os.getenv('ACCOUNT'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        role=os.getenv('ROLE'),
        warehouse=os.getenv('WAREHOUSE'),
        database=os.getenv('DATABASE'),
        schema=os.getenv('SCHEMA'),
    )
    for file_path in get_all_csv_file_path_from_local():
        table_name = file_path.split('/')[-1].split('.')[0]
        con.cursor().execute(f"CREATE OR REPLACE TABLE STAGING.{table_name} AS SELECT * FROM RAW.{table_name}")    
    con.close()

def load_data_into_star_schema():
    con = snowflake.connector.connect(
        account=os.getenv('ACCOUNT'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        role=os.getenv('ROLE'),
        warehouse=os.getenv('WAREHOUSE'),
        database=os.getenv('DATABASE'),
        schema=os.getenv('SCHEMA'),
    )
    for file_path in get_all_csv_file_path_from_local():
        table_name = file_path.split('/')[-1].split('.')[0]
        if table_name.__contains__("listing"):
            con.cursor().execute(listing_insert+table_name) 
            con.cursor().execute(host_insert+table_name) 
    con.close()
def load_fact_table_into_star_schema():
        con = snowflake.connector.connect(
        account=os.getenv('ACCOUNT'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        role=os.getenv('ROLE'),
        warehouse=os.getenv('WAREHOUSE'),
        database=os.getenv('DATABASE'),
        schema=os.getenv('SCHEMA'),
    ) 
    con.cursor().execute(fact_table_query) 
    con.close()

def Copy_Data_From_Stage_To_Datawarehouse():
    con = snowflake.connector.connect(
        account=os.getenv('ACCOUNT'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        role=os.getenv('ROLE'),
        warehouse=os.getenv('WAREHOUSE'),
        database=os.getenv('DATABASE'),
        schema=os.getenv('SCHEMA'),
    )
    stage_tables = ["STG_LISTING_d","STG_HOSTING_d","STG_Census_d","STG_SUBURB_d","STG_LGA_CODE_d","STG_AT3_f"]
    for table_name in stage_tables:
        table = table_name.replace("STG_","")
        con.cursor().execute(f"CREATE OR REPLACE TABLE DATAWAREHOUSE.{table} AS SELECT * FROM STAGING.{table_name}")    
    con.close()

def fetch_pandas(conn,sql):
    n=100000
    cursor = conn.cursor()
    cursor.execute(sql)
    while True:
        dat = cursor.fetchmany(n)
        if not dat:
            break
        a = [cursor.description[i][0] for i in range(len(cursor.description))]
        df = pd.DataFrame(dat, columns=a)
    return df
def export_datamart_table():
    con = snowflake.connector.connect(
        account=os.getenv('ACCOUNT'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        role=os.getenv('ROLE'),
        warehouse=os.getenv('WAREHOUSE'),
        database=os.getenv('DATABASE'),
        schema=os.getenv('SCHEMA'),
    )
    data_mart_table = ["dm_listing_neighbourhood","dm_property_type","dm_host_neighbourhood"] 
    for table_name in data_mart_table:
        dta = fetch_pandas(con,f"SELECT * FROM DATAMART.{table_name}")
        dta.to_csv(f"gs://asia-southeast1-bde-59c7a637-bucket/dags/datamart_files/{table_name}.csv")
    con.close()
#########################################################
#
#   DAG Operator Setup
#
#########################################################
dag_default_args = {
    'owner': 'BDE_LAB_6',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
}

with DAG(
    dag_id=DAG_ID,
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
) as dag:
    truncate_all_schemas_tables = SnowflakeOperator(
        task_id='truncate_all_schemas_tables',
        sql='/part0_truncate_table.sql',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="RAW"
    )
    load_into_snowflake = PythonOperator(
        task_id = "loading_raw_data_into_snowflake",
        python_callable =  Load_Raw_Data
    )
    copy_data_from_raw_to_stage = PythonOperator(
        task_id = "copy_data_from_raw_to_stage",
        python_callable =  Copy_Data_From_RAW_To_Stage
    )
    create_star_schema = SnowflakeOperator(
        task_id='create_star_schema',
        sql='/CreatingDataWarehouse.sql',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="STAGING"
    )
    load_data_star_schema = PythonOperator(
        task_id = "load_data_star_schema",
        python_callable =  load_data_into_star_schema
    )
    load_fact_table = PythonOperator(
        task_id = "load_fact_table_into_star_schema",
        python_callable =  load_fact_table_into_star_schema
    )
    Copy_Data_From_Stage_To_DW = PythonOperator(
        task_id = "Copy_Data_From_Stage_To_Datawarehouse",
        python_callable =  Copy_Data_From_Stage_To_Datawarehouse
    )
    create_dm_tables_in_datamart = SnowflakeOperator(
        task_id='create_dm_tables_in_datamart',
        sql='/data_mart_script.sql',
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="DATAMART"
    )
    export_datamart = PythonOperator(
        task_id = "export_datamart_table",
        python_callable =  export_datamart_table
    )
truncate_all_schemas_tables>>load_into_snowflake>>copy_data_from_raw_to_stage>>create_star_schema>>load_data_star_schema>>load_fact_table>>Copy_Data_From_Stage_To_DW>>create_dm_tables_in_datamart>>export_datamart