from pathlib import Path
from typing import List

import pandas
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / '.env')


def replace_repo(ti):
    data = ti.xcom_pull(task_ids="get_repo")
    URL_DB = os.getenv("URL_DB")
    print(data)

    engine = create_engine(URL_DB)
    df = pandas.DataFrame(data)
    df.to_sql('repo', engine, if_exists='append', index=False)

