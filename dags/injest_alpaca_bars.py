"""
This module contains the 'injest_alpaca_bars' dag. The first operator checks if
the NYSE is open and short circuits if it is closed. If the stock exchange is
open it proceeds with a python operator that pulls bars from the current day 
and pushes them to bigquery.
"""
import logging
from datetime import datetime, timedelta, date
from google.cloud import bigquery
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowSkipException
from util.core.alpaca.alpaca_actions import pull_bars
from util.core.calendar.calendar_util import check_if_exchange_open

EMAILS = ["austin.paxton007@gmail.com", "fallonjoey1@gmail.com"]
ALPACA_HEADER = (
    Variable.get("SECRET_ALPACA_API_KEY_ID"),
    Variable.get("SECRET_ALPACA_API_SECRET_KEY"),
)
TABLE_ID = "cqs-mvp.stocks.price-history"
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

def pull_bars_push_to_bq(**ctx):
    """
    This function bulls bars for Alpaca-supported exchanges and then adds them
    to a BigQuery. Note that the start date is non-inclusive. Also, the end date 
    must be specified 15 minutes before current time for our current market data 
    subscription. 
    """
    yesterday = datetime.now() - timedelta(days=1)
    logging.info(yesterday)
    today = datetime.now() - timedelta(minutes=15)
    logging.info(today)
    bars_df = pull_bars(yesterday, today, ALPACA_HEADER)
    if not bars_df:
        logging.error("NULL DATA")
        raise AirflowSkipException
    logging.info(bars_df)
    client = bigquery.Client(project="cqs-mvp")
    job_config = bigquery.LoadJobConfig(schema=SCHEMA)
    job = client.load_table_from_dataframe(bars_df, TABLE_ID, job_config=job_config)
    # waits for job to complete
    job.result()

def is_exchange_open_today():
    """
    Checks if New York Stock Exchange is open on the current date. Returns 
    boolean value.
    """
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
