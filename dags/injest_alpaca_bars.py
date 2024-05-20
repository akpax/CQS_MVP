import os
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from google.cloud import bigquery

import pendulum
import pandas as pd
from datetime import datetime, timedelta

import logging

# sys.path.insert(0, os.getenv("ROOT_DIR"))

from util.core.alpaca.alpaca_actions import pull_bars

EMAILS = ["austin.paxton007@gmail.com", "fallonjoey1@gmail.com"]

ALPACA_HEADER = (
    Variable.get("SECRET_ALPACA_API_KEY_ID"),
    Variable.get("SECRET_ALPACA_API_SECRET_KEY"),
)

SCHEMA = [
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

TABLE_ID = "cqs-mvp.stocks.price-history"


def pull_bars_push_to_bq(**ctx):
    yesterday = ctx["execution_date"] - timedelta(days=5)
    logging.info(yesterday)
    today = ctx["next_execution_date"] - timedelta(days=4)
    logging.info(today)
    bars_df = pull_bars(yesterday, today, ALPACA_HEADER)
    if len(bars_df) == 0:
        logging.error("NULL DATA")
        1 / 0
    logging.info(bars_df)
    client = bigquery.Client(project="cqs-mvp")
    job_config = bigquery.LoadJobConfig(schema=SCHEMA)
    job = client.load_table_from_dataframe(bars_df, TABLE_ID, job_config=job_config)
    # waits for job to complete
    job.result()


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
    schedule="5 7 * * 1-5",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["injest_data"],
    dag_id="injest_alpaca_bars",
)

first_task = EmptyOperator(task_id="first_task", dag=dag)

pull_and_push_task = PythonOperator(
    task_id="pull_and_push_task", python_callable=pull_bars_push_to_bq, dag=dag
)

# email_task = EmailOperator(
#     task_id="email_task",
#     to=EMAILS,
#     subject="[SUCCESS] DAILY ALPACA BARS LOADED INTO BQ",
#     html_content=" ",
#     dag=dag,
# )


first_task >> pull_and_push_task  # >> email_task
