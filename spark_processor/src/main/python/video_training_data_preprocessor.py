from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import window, collect_list, pandas_udf, PandasUDFType, concat, udf, from_json, decode, length, rand
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, BinaryType, BooleanType
import os
import sys
import cv2
import numpy as np


def main():
    """
    Read images from a Pravega stream, randomly reorder them, and write to JSON files.
    These JSON files can then be used to training a machine learning model.
    JSON files are universally compatible but require images to be base64 encoded.
    For optimal performance, other encodings should be used such as Avro, Parquet, or TensorFlow TFRecords.
    """
    print(sys.version)
    spark = (SparkSession
             .builder
             .appName('test1')
             .getOrCreate()
             )
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    output_dir = '/tmp/video_training_data'
    df = (spark
          .read
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .option("encoding", "chunked_v1")
          .load()
          )
    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    schema='timestamp timestamp, frame_number int, camera int, ssrc int, data binary'
    df = df.select('*', from_json('event_string', schema=schema).alias('event'))
    df = df.select('*', 'event.*')
    df = df.drop('raw_event', 'event_string', 'event')
    (df
        .orderBy(rand(seed=1))
        .write
        .mode('overwrite')
        .format('json')
        .save(output_dir)
     )


if __name__ == '__main__':
    main()
