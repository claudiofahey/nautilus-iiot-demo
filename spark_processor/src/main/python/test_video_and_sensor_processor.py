from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import window, collect_list, pandas_udf, PandasUDFType, concat, udf, from_json, decode, length
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, BinaryType, BooleanType
import os
import sys
import cv2
import numpy as np


def main():
    print(sys.version)
    spark = (SparkSession
             .builder
             .appName('test1')
             .getOrCreate()
             )
    spark.conf.set('spark.sql.shuffle.partitions', '1')
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
    test(spark)


def test(spark):
    """
    This demonstrates reading large images from Pravega and detecting defects.
    The data field contains a base-64 encoded PNG image file.
    It uses chunked encoding to support events of 2 GiB.
    """
    schema='timestamp timestamp, frame_number int, camera int, ssrc int, data binary'

    # To allow for large images and avoid out-of-memory, the JVM will
    # send to the Python UDF this batch size.
    spark.conf.set('spark.sql.execution.arrow.maxRecordsPerBatch', '1')

    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    checkpoint_location = os.getenv('CHECKPOINT_LOCATION', '/tmp/spark_checkpoints_test_video_and_sensor_processor')

    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .option("encoding", "chunked_v1")
          .load()
          )

    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema).alias('event'))
    df = df.select('*', 'event.*')
    df = df.select('*', length('data'))
    df = df.withWatermark('timestamp', '60 second')

    @pandas_udf(returnType=DoubleType(), functionType=PandasUDFType.SCALAR)
    def defect_probability(s):
        """Calculate the probability of a defect."""
        def f(data):
            # Decode the image.
            numpy_array = np.frombuffer(data, dtype='uint8')
            rgb = cv2.imdecode(numpy_array, -1)
            # Perform a computation on the image to determine the probability of a defect.
            # For now, we just calculate the mean pixel value.
            # We can use any Python library, including NumPy and TensorFlow.
            p = rgb.mean() / 255.0
            return p
        return s.apply(f)

    df = df.select('*', defect_probability('data').alias('defect_probability'))

    df = df.drop('raw_event', 'event_string', 'event', 'data')

    df.printSchema()

    if True:
        (df
         .writeStream
         .trigger(processingTime='3 seconds')    # limit trigger rate
         .outputMode('append')
         .format('console')
         .option('truncate', 'false')
         .option('checkpointLocation', checkpoint_location)
         .start()
         .awaitTermination()
         )


def test2(spark):
    """
    """
    schema='timestamp timestamp, frame_number int, camera int, ssrc int, data binary'

    # To allow for large images and avoid out-of-memory, the JVM will
    # send to the Python UDF this batch size.
    spark.conf.set('spark.sql.execution.arrow.maxRecordsPerBatch', '1')

    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')

    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .option("encoding", "chunked_v1")
          .load()
          )

    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema).alias('event'))
    df = df.select('*', 'event.*')
    df = df.select('*', length('data'))
    df = df.withWatermark('timestamp', '60 second')

    def f(batch_df, batch_id):
        print('batch_id=%d' % batch_id)
        png0 = batch_df.select('data').limit(1).collect()[0][0]
        print('png0=%s' % png0[0:20])
    #     IPython.display.clear_output(wait=True)
    #     IPython.display.display(IPython.display.Image(data=png0))

    (df
     .writeStream
     .trigger(processingTime='3 seconds')  # limit trigger rate
     .foreachBatch(f)
     .start()
     .awaitTermination()
     )


def test_batch(spark):
    """
    Test of Spark SQL batch mode.
    """
    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    df = (spark
          .read
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .option("encoding", "chunked_v1")
          .load()
          )
    df.show()


def test1(spark):
    """
    This demonstrates reading large images from Pravega and detecting defects.
    The data field contains a base-64 encoded PNG image file.
    It uses chunked encoding to support events of 2 GiB.
    This runs out of memory because the non-Pandas runner uses fixed batches of 100.
    """
    schema='timestamp timestamp, frame_number int, camera int, ssrc int, data binary'

    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .option("encoding", "chunked_v1")
          .load()
          )

    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema).alias('event'))
    df = df.select('*', 'event.*')
    df = df.select('*', length('data'))
    # df = df.withWatermark('timestamp', '60 second')

    @udf(returnType=DoubleType())
    def defect_probability(data):
        """Calculate the probability of a defect."""
        # Decode the image.
        rgb = cv2.imdecode(np.array(data), -1)
        # Perform a computation on the image to determine the probability of a defect.
        # For now, we just calculate the mean pixel value.
        # We can any Python library, including NumPy and TensorFlow.
        p = rgb.mean() / 255.0
        return float(p)

    df = df.select('*', defect_probability('data').alias('defect_probability'))

    df = df.drop('raw_event', 'event_string', 'event', 'data')

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


if __name__ == '__main__':
    main()
