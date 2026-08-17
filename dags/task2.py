from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="task_2",
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    generation = SparkSubmitOperator(
        task_id='generation',
        application="/opt/airflow/dags/scripts/data_generation.py",
        conn_id="spark_default"
    )
