from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import os

from scripts import get_repo_order_stars, replace_repo, sum_stars, forks_to_stars

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

with DAG(
    dag_id="stars_dag",
    start_date=datetime(2026, 1, 1),
    schedule=os.getenv("TIME"),
    catchup=False
) as dag:

    get_repo = PythonOperator(
        task_id="get_repo",
        python_callable=get_repo_order_stars,
        op_args=[int(os.getenv("N", 10))]
    )

    replace_repo_db = PythonOperator(task_id="replace_repo_db",
                                     python_callable=replace_repo,
                                     do_xcom_push=True)

    transform_data_stars = PythonOperator(task_id="transform_data_stars",
                                    python_callable=sum_stars)
    transform_data_fork = PythonOperator(task_id="transform_data_fork",
                                    python_callable=forks_to_stars)

    get_repo >> replace_repo_db >> transform_data_stars >> transform_data_fork

