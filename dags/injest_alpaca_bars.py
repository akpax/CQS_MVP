import os
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator

from google.cloud import bigquery

import pendulum
import pandas as pd
from datetime import datetime, timedelta, date

import logging


from util.core.alpaca.alpaca_actions import pull_bars
from util.core.calendar.calendar_util import check_if_exchange_open

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
    yesterday = datetime.now() - timedelta(days=1)
    logging.info(yesterday)
    today = datetime.now() - timedelta(minutes=15)
    logging.info(today)
    bars_df = pull_bars(yesterday, today, ALPACA_HEADER)
    if len(bars_df) == 0:
        logging.error("NULL DATA")
        # TODO raise airflow skip exception
    logging.info(bars_df)
    client = bigquery.Client(project="cqs-mvp")
    job_config = bigquery.LoadJobConfig(schema=SCHEMA)
    job = client.load_table_from_dataframe(bars_df, TABLE_ID, job_config=job_config)
    # waits for job to complete
    job.result()


def is_exchange_open_today():
    return check_if_exchange_open("NYSE", date.today())


dag = DAG(
    schedule="5 7 * * 1-5",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["injest_data"],
    dag_id="injest_alpaca_bars",
)

first_task = EmptyOperator(task_id="first_task", dag=dag)

check_market_open_task = ShortCircuitOperator(
    task_id="check_market_open_task", python_callable=is_exchange_open_today
)

pull_and_push_task = PythonOperator(
    task_id="pull_and_push_task", python_callable=pull_bars_push_to_bq, dag=dag
)

# email_task = EmailOperator(
#     task_id="email_task",
#     to=EMAILS,
#     subject="[SUCCESS] DAILY ALPACA BARS LOADED INTO BQ",
#     html_content=" ",
#     dag=dag,
# )``


first_task >> check_market_open_task >> pull_and_push_task  # >> email_task
