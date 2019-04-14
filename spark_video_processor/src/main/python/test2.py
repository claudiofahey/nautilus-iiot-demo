from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import window, collect_list, pandas_udf, PandasUDFType, concat, udf, from_json, decode
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, BinaryType, BooleanType
import os
import sys
import zlib
import struct


def main():
    print(sys.version)
    spark = (SparkSession
             .builder
             .appName('test1')
             .getOrCreate()
             )
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
    spark.conf.set('spark.sql.shuffle.partitions', '1')
    test12(spark)


def test12(spark):
    # ssrc is the synchronization source identifier. See https://en.wikipedia.org/wiki/Real-time_Transport_Protocol.
    # It should be selected at random by each process that writes records.
    schema='timestamp timestamp, frame_number int, camera int, chunk int, num_chunks int, ssrc int, data binary'

    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .load()
          )

    # Decode JSON event.
    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema, options=dict(mode='FAILFAST')).alias('event'))
    df = df.select('*', 'event.*')

    df = df.withWatermark('timestamp', '60 second')

    # The number of chunks must be fixed for the entire Spark job because it determines the number of joins.
    num_chunks = 3
    # Ignore any records with a different number of chunks. Perhaps these can be sent to an error stream.
    df = df.filter(df.num_chunks == num_chunks)
    # Create a dataframe for each chunk.
    chunk_dfs = [df.filter(df.chunk == chunk_index).drop('chunk').withColumnRenamed('data', 'data%d' % chunk_index)
                 for chunk_index in range(num_chunks)]
    # Join chunks.
    df = chunk_dfs[0]
    for chunk_id in range(1, num_chunks):
        df = df.join(chunk_dfs[chunk_id], ['timestamp', 'camera', 'ssrc'], 'inner')
    # Concatenate binary data.
    data_cols = ['data%d' % chunk_index for chunk_index in range(num_chunks)]
    df = df.select('timestamp', 'camera', 'ssrc', concat(*data_cols).alias('data'))
    # Deduplication.
    df = df.dropDuplicates(['timestamp', 'camera'])

    @udf(returnType=BinaryType())
    def parse_checksum(checksum_and_data):
        return checksum_and_data[0:4]

    @udf(returnType=BinaryType())
    def parse_data(checksum_and_data):
        return checksum_and_data[4:]

    @udf(returnType=BooleanType())
    def is_checksum_correct(checksum, data):
        expected = struct.unpack('!I', checksum)[0]
        calculated = zlib.crc32(data)
        print('expected=%d, calculated=%d' % (expected, calculated))
        return expected == calculated

    df = df.withColumnRenamed('data', 'checksum_and_data')
    df = df.select('*', parse_checksum('checksum_and_data').alias('checksum'), parse_data('checksum_and_data').alias('data'))
    df = df.select('*', is_checksum_correct('checksum', 'data').alias('is_checksum_correct'))
    # df = df.filter(df.is_checksum_correct == True)

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


def test11(spark):
    # ssrc is the synchronization source identifier. See https://en.wikipedia.org/wiki/Real-time_Transport_Protocol.
    # It should be selected at random by each process that writes records.
    schema='timestamp timestamp, frame_number int, camera int, chunk int, num_chunks int, ssrc int, data binary'

    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .load()
          )

    # Decode JSON event.
    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema, options=dict(mode='FAILFAST')).alias('event'))
    df = df.select('*', 'event.*')

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


def test10(spark):
    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .load()
          )

    df.printSchema()

    if True:
        (df
         .writeStream
         .trigger(processingTime='3 seconds')    # limit trigger rate
         .outputMode('append')
         .format('console')
         # .option('truncate', 'false')
         .start()
         .awaitTermination()
         )

main()
