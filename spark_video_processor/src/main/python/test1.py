from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import window, collect_list, pandas_udf, PandasUDFType, concat
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, BinaryType
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
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    test7(spark)


def test7(spark):
    # ssrc is the synchronization source identifier. See https://en.wikipedia.org/wiki/Real-time_Transport_Protocol.
    # It should be selected at random by each process that writes records.
    schema='timestamp timestamp, camera int,  ssrc int, chunk int, num_chunks int, data binary'

    df = (spark
          .readStream
          .json('testdata/test7', schema=schema)
          )
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


def test6(spark):
    schema='timestamp timestamp, camera int, chunk int, data binary'

    df = (spark
          .readStream
          .json('testdata/test5', schema=schema)
          )
    df = df.withWatermark('timestamp', '60 second')

    num_chunks = 3
    chunk_dfs = [df.filter(df.chunk == chunk_index).drop('chunk').withColumnRenamed('data', 'data%d' % chunk_index)
                 for chunk_index in range(num_chunks)]
    df = chunk_dfs[0]
    for chunk_id in range(1, num_chunks):
        df = df.join(chunk_dfs[chunk_id], ['timestamp', 'camera'], 'inner')
    data_cols = ['data%d' % chunk_index for chunk_index in range(num_chunks)]
    df = df.select('timestamp', 'camera', concat(*data_cols).alias('data'))

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


def test5(spark):
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("camera", IntegerType(), False),
        StructField("chunk", IntegerType(), False),
        StructField("data", BinaryType(), False),
    ])
    #schema='timestamp timestamp, camera int, chunk int, data double'

    df = (spark
          .readStream
          .json('testdata/test5', schema=schema)
          )

    df = df.withWatermark('timestamp', '60 second')
    df0 = df.filter(df.chunk == 0).drop('chunk').withColumnRenamed('data', 'data0')
    df1 = df.filter(df.chunk == 1).drop('chunk').withColumnRenamed('data', 'data1')
    df = df0.join(df1, ['timestamp', 'camera'], 'inner')
    df = df.select('timestamp', 'camera', concat('data0', 'data1').alias('data'))

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


def test4(spark):
    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("camera", IntegerType(), False),
        StructField("chunk", IntegerType(), False),
        StructField("data", DoubleType(), False),
    ])
    #schema='timestamp timestamp, camera int, chunk int, data double'

    df = (spark
          .readStream
          .json('testdata/test1', schema=schema)
          )

    df = df.withWatermark('timestamp', '60 second')
    df0 = df.filter(df.chunk == 0).drop('chunk').withColumnRenamed('data', 'data0')
    df1 = df.filter(df.chunk == 1).drop('chunk').withColumnRenamed('data', 'data1')
    df = df0.join(df1, ['timestamp', 'camera'], 'inner')

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


# Error: Queries with streaming sources must be executed with writeStream.start()
def test3(spark):
    @pandas_udf('double', PandasUDFType.GROUPED_AGG)
    def reassemble(v):
        # print(v)
        return v.sum()

    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("camera", IntegerType(), False),
        StructField("chunk", IntegerType(), False),
        StructField("data", DoubleType(), False),
    ])
    #schema='timestamp timestamp, camera int, chunk int, data double'

    df = (spark
          .readStream
          .json('testdata/test1', schema=schema)
          )

    print(df.rdd)


# Error: Streaming aggregation doesn't support group aggregate pandas UDF
def test2(spark):
    @pandas_udf('double', PandasUDFType.GROUPED_AGG)
    def reassemble(v):
        # print(v)
        return v.sum()

    schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("camera", IntegerType(), False),
        StructField("chunk", IntegerType(), False),
        StructField("data", DoubleType(), False),
    ])
    #schema='timestamp timestamp, camera int, chunk int, data double'

    df = (spark
        .readStream
        .json('testdata/test1', schema=schema)
        )

    # group by camera, aggregate data
    df = (df
        .withWatermark('timestamp', '60 second')
        .groupBy(
            # window('timestamp', '10 minutes'),
            'timestamp',
            'camera')
        # .sum('data')
        # .sortWithinPartitions('chunk')
        # .agg(collect_list('data'))
        .agg(reassemble(df.data))
        )

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
    df = spark.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
        ("id", "v"))
    @pandas_udf("double", PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
    def mean_udf(v):
        return v.mean()
    w = Window \
        .partitionBy('id') \
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df.withColumn('mean_v', mean_udf(df['v']).over(w)).show()

main()
