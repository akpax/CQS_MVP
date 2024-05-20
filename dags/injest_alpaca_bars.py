import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from google.cloud import bigquery

import pendulum
import pandas as pd
from datetime import datetime

# sys.path.insert(0, os.getenv("ROOT_DIR"))

from util.core.alpaca.alpaca_actions import pull_bars

schema = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("open", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("high", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("low", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("close", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("volume", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("trade_count", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("vwap", "FLOAT", mode="REQUIRED"),
]

table_id = "cqs-mvp.stocks.price-history"


def pull_bars_push_to_bq(ti):
    bars_df = pull_bars(datetime(2024, 5, 15), datetime(2024, 5, 16))
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(bars_df, table_id, job_config=job_config)
    # waits for job to complete
    job.result()
    pass


"""
Need to unpack context (**ctx)
Add retry to pull_bars, 
need to add email on retry (default) args

Operators: 
dummy operator: flag, softball to scheduler
python operator: contains pull bars function and biq query client (load_table_from_data_frame)
- add additional column to data frame DATEGENERATED
 - make sure to not include indexes, in bq make so they have their own tables

May need to add Google cloud credentials to airflow Connections in admin panel -> try without it first

authentication issues w bq Client Class?
try to hit first with random data set
-if i cant hit check out LoadJob class 
"""

# def transform(ti=None, **kwargs):
#     temp_csv = ti.xcom_pull(task_ids="extract")
#     df = pd.read_csv(temp_csv)
#     print(df)
#     os.remove(temp_csv)


dag = DAG(
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["injest_data"],
    dag_id="injest_alpaca_bars",
)

first_task = EmptyOperator(task_id="empty", dag=dag)

transform_task = PythonOperator(
    task_id="pull_and_push", python_callable=pull_bars_push_to_bq, dag=dag
)

send_status_task = EmailOperator(task_id="send_status")


first_task >> transform_task
