from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import window, collect_list, pandas_udf, PandasUDFType, concat, udf, from_json, decode, length, expr
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, BinaryType, BooleanType
import os
import sys


def main():
    print(sys.version)
    spark = (SparkSession
             .builder
             .appName('test1')
             .getOrCreate()
             )
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
    spark.conf.set('spark.sql.shuffle.partitions', '1')
    test1(spark)


def test2(spark):
    """
    Stream to stream join based on a timestamp range
    """
    schema='timestamp timestamp, event_type string, device_id string, temp_celsius double'

    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "sensors")
          .load()
          )

    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema).alias('event'))
    df = df.select('*', 'event.*')
    df = df.drop('raw_event', 'event_string', 'event')
    df = df.withWatermark('timestamp', '60 second')
    df = df.withColumnRenamed('device_id', 'device_id_1')
    df = df.withColumnRenamed('timestamp', 'timestamp_1')

    df1 = df

    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "sensors2")
          .load()
          )

    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema).alias('event'))
    df = df.select('*', 'event.*')
    df = df.drop('raw_event', 'event_string', 'event')
    df = df.withWatermark('timestamp', '60 second')
    df = df.withColumnRenamed('device_id', 'device_id_2')
    df = df.withColumnRenamed('timestamp', 'timestamp_2')

    df2 = df

    df = df1.join(df2, expr(
        'device_id_1 = device_id_2 and '
        'timestamp_1 >= timestamp_2 and '
        'timestamp_1 < timestamp_2 + interval 2 second'))

    df.printSchema()

    if True:
        (df
         .writeStream
         .trigger(processingTime='3 seconds')    # limit trigger rate
         .outputMode('append')
         .format('console')
         .option('truncate', 'false')
         .start()
         .awaitTermination()
         )

def test1(spark):
    """
    This demonstrates reading JSON events from Pravega.
    """
    schema='timestamp timestamp, event_type string, device_id string, temp_celsius double'

    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "sensors")
          .load()
          )

    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema).alias('event'))
    df = df.select('*', 'event.*')
    df = df.drop('raw_event', 'event_string', 'event')
    df = df.withWatermark('timestamp', '60 second')

    df.printSchema()

    if True:
        (df
         .writeStream
         .trigger(processingTime='3 seconds')    # limit trigger rate
         .outputMode('append')
         .format('console')
         .option('truncate', 'false')
         .option('checkpointLocation', '/tmp/spark_checkpoints_test_sensor_processor')
         .start()
         .awaitTermination()
         )


if __name__ == '__main__':
    main()
