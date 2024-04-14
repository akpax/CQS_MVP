# CQS_MVP
Designing MVP  Airflow DAG for data injestion and storage

To start Airflow run the following commands in your terminal

cd ~/CQS_MVP
poetry shell
poetry install
export AIRFLOW_HOME=`(pwd)`
airflow standalone
airflow dags list