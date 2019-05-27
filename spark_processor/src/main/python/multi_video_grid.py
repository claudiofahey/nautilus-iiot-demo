from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as func
from pyspark.sql.functions import window, collect_list, pandas_udf, PandasUDFType, concat, udf, from_json, decode, length
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, BinaryType, BooleanType
import os
import sys
# import cv2
import numpy as np
import shutil
from PIL import Image, ImageDraw, ImageFont
import io
import math
import pandas as pd


def main():
    print(sys.version)
    spark = (SparkSession
             .builder
             .getOrCreate()
             )
    spark.conf.set('spark.sql.shuffle.partitions', '1')
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
    run(spark)


def run(spark):
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
    checkpoint_location = os.getenv('CHECKPOINT_LOCATION', '/tmp/spark_checkpoints_multi_video_grid')
    shutil.rmtree(checkpoint_location, ignore_errors=True)

    df = (spark
          .readStream
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "video")
          .option("encoding", "chunked_v1")
          # .option("start_stream_cut", "earliest")
          .load()
          )

    df = df.withColumnRenamed('event', 'raw_event')
    df = df.select('*', decode('raw_event', 'UTF-8').alias('event_string'))
    df = df.select('*', from_json('event_string', schema=schema).alias('event'))
    df = df.select('*', 'event.*')
    df = df.select('*', length('data'))
    df = df.withWatermark('timestamp', '1 second')
    df = df.drop('raw_event', 'event_string', 'event')

    grp = df.groupby(
            # window('timestamp', '1 second'),
            'frame_number',
    )
    #df = df.agg(func.collect_list(func.array(df['camera'], df['data'])).alias('cameras'))

    # @pandas_udf(returnType='frame_number int, data binary', functionType=PandasUDFType.GROUPED_MAP)
    # def combine_thumbnails(df):
    #     """Input is a Pandas dataframe with 1 row per camera and frame.
    #     Output should be a Pandas dataframe with 1 row per frame."""
    #     print(f'combine_thumbnails: s={df}')
    #     df.info(verbose=True)
    #
    #     return df[['frame_number', 'data']]

    @pandas_udf(returnType='timestamp timestamp, frame_number int, ssrc int, data binary', functionType=PandasUDFType.GROUPED_MAP)
    def combine_images_into_grid(df):
        if df.empty:
            return None
        # Get first image to determine height, width.
        row0 = df.iloc[0]
        image0_png_bytes = row0['data']
        image0_pil = Image.open(io.BytesIO(image0_png_bytes))
        num_cameras = df.camera.max() + 1
        # Determine number of images per row and column.
        grid_count = math.ceil(math.sqrt(num_cameras))
        image_width = image0_pil.width + 1  # add 1 for margin between images
        image_height = image0_pil.height + 1 # add 1 for margin between images
        # Create blank output image, white background.
        out_pil = Image.new('RGB', (image_width * grid_count - 1, image_height * grid_count - 1), (255,255,255))
        def add_image(r):
            in_pil = Image.open(io.BytesIO(r['data']))
            x = (r['camera'] % grid_count) * image_width
            y = (r['camera'] // grid_count) * image_width
            out_pil.paste(in_pil, (x, y))
        df.apply(add_image, axis=1)
        out_bytesio = io.BytesIO()
        out_pil.save(out_bytesio, format='PNG')
        out_bytes = out_bytesio.getvalue()
        new_row = row0[['timestamp', 'frame_number']]
        new_row['ssrc'] = 0
        new_row['data'] = out_bytes
        return pd.DataFrame([new_row])

    # @pandas_udf(returnType=DoubleType(), functionType=PandasUDFType.SCALAR)
    # def combine_thumbnails(s):
    #     print(f'combine_thumbnails: s={s}')
    #     def f(data):
    #         print('combine_thumbnails: data')
    #         # # Decode the image.
    #         # numpy_array = np.frombuffer(data, dtype='uint8')
    #         # rgb = cv2.imdecode(numpy_array, -1)
    #         # # Perform a computation on the image to determine the probability of a defect.
    #         # # For now, we just calculate the mean pixel value.
    #         # # We can use any Python library, including NumPy and TensorFlow.
    #         # p = rgb.mean() / 255.0
    #         return 3.14
    #     return s.apply(f)

    df = grp.apply(combine_images_into_grid)
    # df = df.select('*', combine_thumbnails('cameras').alias('combined'))
    df = df.select(func.to_json(func.struct(df["frame_number"], df["data"])).alias("event"))

    df.printSchema()

    if False:
        (df
         .writeStream
         .trigger(processingTime='3 seconds')    # limit trigger rate
         .outputMode('append')
         .format('console')
         .option('truncate', 'true')
         .option('checkpointLocation', checkpoint_location)
         .start()
         .awaitTermination()
         )
    else:
        (df
        .writeStream
        .trigger(processingTime="200 milliseconds")
        .outputMode("append")
        .format("pravega")
        .option("controller", controller)
        .option("scope", scope)
        .option("stream", "combinedvideo")
        .option("checkpointLocation", checkpoint_location)
        .start()
        .awaitTermination()
         )


if __name__ == '__main__':
    main()
