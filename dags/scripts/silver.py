from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def silver_processing():
    spark = SparkSession.builder.master("local[*]").appName("generation").getOrCreate()

    df = spark.read.parquet("/opt/airflow/data/my_dataset.parquet")
    df = df.dropna()
    df = df.filter(F.col('bank_card') != F.col('transaction_bank_card'))\
        .filter(F.col('transaction_money') != 0)\
        .filter(F.col('status') == 'success')\
        .filter(F.col('bank_card').rlike(r'^[*]{12}\d{4}'))\
        .filter(F.col('bank_card').rlike(r'^[*]{12}\d{4}'))
    df.show()
    n = df.count()
    print(f"cnt {n}")

silver_processing()