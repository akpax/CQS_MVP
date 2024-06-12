# CQS_MVP

Designing MVP  Airflow DAG for data injestion and storage

To start Airflow run the following commands in your terminal

## Running Airflow 2.9.0

cd ~/CQS_MVP

poetry shell

poetry install

export AIRFLOW_HOME=`pwd`

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=`pwd`/local/airflow.db

airflow standalone

(*new shell*)

poetry shell

export AIRFLOW_HOME=`(pwd)`

airflow dags list

## ADDING DEPENENCIES

poetry shell

poetry add some_pacakage

poetry install

some_package --version

## DB pointer

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(pwd)/local/airflow.db
