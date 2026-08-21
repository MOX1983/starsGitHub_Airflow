from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def generation():
    spark = SparkSession.builder.master("local[*]").appName("generation").getOrCreate()

    df = spark.range(1, 8_000_000)

    filter_acc = F.when(F.rand() < 0.05, None)\
        .otherwise(F.lpad((F.rand() * 10_000 + 1000).cast("bigint").cast("string"), 16, '*'))
    df = df.withColumn('bank_card', filter_acc)

    filter_t = F.when(F.rand() < 0.05, None).otherwise(F.round(F.randn() / F.rand() * 100, 2))
    df = df.withColumn('transaction_money', filter_t)

    r = F.rand()
    filter_curr = F.when(r < 0.05, None)\
        .when(r < 0.23, F.lit('BYN'))\
        .when(r < 0.46, F.lit('RUB'))\
        .when(r < 0.89, F.lit('USD'))\
        .otherwise(F.lit('EUR'))
    df = df.withColumn('currency', filter_curr)

    df = df.withColumn('date', F.from_unixtime(
        F.unix_timestamp(F.current_timestamp()) - F.round((F.rand() * 1_000_000)))
                       )

    filter_acc = F.when(F.rand() < 0.05, None)\
        .otherwise(F.lpad((F.rand() * 1000 + 1000).cast("bigint").cast("string"), 16, '*'))
    df = df.withColumn('transaction_bank_card', filter_acc)

    df = df.withColumn('status', F.when(F.rand() < 0.3, 'failed').otherwise('success'))

    df.write.mode("overwrite").parquet("/opt/airflow/data/my_dataset.parquet")
    spark.stop()

generation()
