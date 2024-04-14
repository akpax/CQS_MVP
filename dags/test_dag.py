from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


# [START dag_decorator_usage]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    dag_display_name="TEST_DAG_EXAMPLE",
)
def example_display_name():
    sample_task_1 = EmptyOperator(
        task_id="sample_task_1",
        task_display_name="Sample Task 1",
    )

    @task(task_display_name="Sample Task 2")
    def sample_task_2():
        pass

    sample_task_1 >> sample_task_2()


example_dag = example_display_name()
# [END dag_decorator_usage]