from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pathlib import Path

from pyspark.sql import functions as F

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

def sum_stars():
    spark = SparkSession.builder.master("local[*]").appName('Transform_repo') \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1").getOrCreate()

    df = spark.read.jdbc(JDBC_URL, DBTABLE, properties=properties)

    stars_sum = df.groupBy(F.col("owner_login"))\
        .agg(F.sum('stars').alias("sum_stars"), F.count("id").alias("cnt_repo"), F.avg("forks").alias("avg_forks"))\
        .orderBy(F.col("sum_stars").desc())

    stars_sum.show()

    spark.stop()

def forks_to_stars():
    spark = SparkSession.builder.master("local[*]").appName('Transform_repo') \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1").getOrCreate()

    df = spark.read.jdbc(JDBC_URL, DBTABLE, properties=properties)

    forks_stars = df.groupBy(F.col("owner_login"))\
        .agg(F.round(F.sum('forks') / F.sum('stars'),5).alias("forks_to_stars"),
             F.when((F.sum('forks') / F.sum('stars')) > 0.3, 'High Engagement').otherwise('Low Engagement').alias("status"))\
        .orderBy(F.col("forks_to_stars").desc())

    forks_stars.show()

    spark.stop()