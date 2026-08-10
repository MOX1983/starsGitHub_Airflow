from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from yaml import safe_load

from scripts import get_repo_order_stars, replace_repo, sum_stars, forks_to_stars

CONFIG_PATH = Path(__file__).resolve().parent / 'config.yml'
def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return safe_load(f)


with DAG(
    dag_id="stars_dag",
    start_date=datetime(2026, 1, 1),
    schedule=load_config()['time'],
    catchup=False
) as dag:

    get_repo = PythonOperator(
        task_id="get_repo",
        python_callable=get_repo_order_stars,
        op_args=[int(load_config()['n'])]
    )

    replace_repo_db = PythonOperator(task_id="replace_repo_db",
                                     python_callable=replace_repo,
                                     do_xcom_push=True)

    transform_data_stars = PythonOperator(task_id="transform_data_stars",
                                    python_callable=sum_stars)
    transform_data_fork = PythonOperator(task_id="transform_data_fork",
                                    python_callable=forks_to_stars)

    get_repo >> replace_repo_db >> transform_data_stars >> transform_data_fork

