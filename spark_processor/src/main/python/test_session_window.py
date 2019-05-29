from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as func
# from pyspark.sql.functions import window, collect_list, pandas_udf, PandasUDFType, concat, udf, from_json, decode, length
# from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, BinaryType, BooleanType
import os
import sys
import numpy as np
import shutil
import io
import math
import pandas as pd


def main():
    print(sys.version)
    spark = (SparkSession
             .builder
             .getOrCreate()
             )
    spark.conf.set('spark.sql.shuffle.partitions', '4')
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
    run(spark)


def run(spark):
    checkpoint_location = os.getenv('CHECKPOINT_LOCATION', '/tmp/spark-checkpoints-test_session_window')
    shutil.rmtree(checkpoint_location, ignore_errors=True)

    df = (spark
     .readStream
     .format("rate")
     .option("rowsPerSecond", "5")
     .load()
     )

    df.printSchema()

    df = df.selectExpr("timestamp", "cast(value / 5 as int) as key", "value")
    df = df.withWatermark("timestamp", "1 second")

    df.printSchema()

    # window = func.window('timestamp', '2 seconds', '1 second'),
    #
    # grp = df.groupBy(
    #     'key',
    #     func.window('timestamp', '2 seconds', '1 second'),
        # window,
    # )
    # df = grp.agg(func.count(df['value']))

    df.printSchema()



    (df
        .writeStream
        .trigger(processingTime="3 seconds")
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "/tmp/spark-checkpoints-test_session_window")
        .start()
        .awaitTermination()
     )


if __name__ == '__main__':
    main()
