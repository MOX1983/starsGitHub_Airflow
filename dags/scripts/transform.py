from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import Window

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

JDBC_URL = os.getenv("JDBC_URL")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DBTABLE = os.getenv("DBTABLE")
DRIVER = os.getenv("DRIVER")

properties = {
    "user": USER,
    "password": PASSWORD,
    "driver": DRIVER
}

def sum_stars(df):

    stars_sum = df.groupBy(F.col("owner_login"))\
        .agg(F.sum('stars').alias("sum_stars"), F.count("id").alias("cnt_repo"), F.avg("forks").alias("avg_forks"))\
        .orderBy(F.col("sum_stars").desc())

    stars_sum.show()


def forks_to_stars(df):

    forks_stars = df.groupBy(F.col("owner_login"))\
        .agg(F.round(F.sum('forks') / F.sum('stars'),5).alias("forks_to_stars"),
             F.when((F.sum('forks') / F.sum('stars')) > 0.3, 'High Engagement').otherwise('Low Engagement').alias("status"))\
        .orderBy(F.col("forks_to_stars").desc())

    forks_stars.show()

def dynamics_stars(df):
    """
    select owner_login, created_at, stars, coalesce(stars - lag(stars) over(partition by id order by created_at ), 0) as cnt_stars
    from repo
    order by owner_login, created_at asc;
    """
    win_diff = Window.partitionBy('id').orderBy('created_at')
    dynamics = df.withColumn('cnt_stars',
                             F.coalesce(F.col('stars') - F.lag('stars').over(win_diff), F.lit(0))
                             )\
        .select('owner_login', 'created_at', 'stars', 'cnt_stars')\
        .orderBy(F.col('owner_login').desc(), F.col('created_at').asc())

    dynamics.show()

def transform_data():
    spark = SparkSession.builder.master("local[*]").appName('Transform_repo') \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1").getOrCreate()

    df = spark.read.jdbc(JDBC_URL, DBTABLE, properties=properties).cache()
    # я подумала, что это хорошая идея добавить cahce(), чтобы не выполняеть постоянно чтение dataFrame :)

    print("sum_stars")
    sum_stars(df)

    print("forks_to_stars")
    forks_to_stars(df)

    print('dynamics_stars')
    dynamics_stars(df)

    spark.stop()



