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
    spark.conf.set('spark.sql.shuffle.partitions', '4')
    spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
    run(spark)


def run(spark):
    """
    This is an attempt at combining multiple video sources into a grid of images.
    WARNING: This is broken because Spark is not maintaining the time order of the images.
    This file has been superceded by the Flink/Java class MultiVideoGridJob in the flinkprocessor directory.
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
    fps = 2.0
    df = df.selectExpr('*', f'timestamp(floor(cast(timestamp as double) * {fps}) / {fps}) as discrete_timestamp')
    df = df.withWatermark('discrete_timestamp', '5 second')
    df = df.drop('raw_event', 'event_string', 'event')

    thumbnail_size = (84, 84)

    @pandas_udf(returnType='binary', functionType=PandasUDFType.SCALAR)
    def decode_and_scale_image(data_series, ssrc):
        def f(data):
            in_pil = Image.open(io.BytesIO(data))
            out_pil = in_pil.resize(thumbnail_size)
            return out_pil.tobytes()
        return data_series.apply(f)

    df = df.select('*', decode_and_scale_image(df['data'], df['ssrc']).alias('image'))
    df = df.select('*', func.to_json(func.struct(df['discrete_timestamp'], df['frame_number'], df['camera'])).alias('json'))

    df = df.repartition(1)

    grp = df.groupby(
            # window('timestamp', '1 second'),
            'discrete_timestamp',
    )

    @pandas_udf(returnType='timestamp timestamp, frame_number int, ssrc int, data binary, source string', functionType=PandasUDFType.GROUPED_MAP)
    def combine_images_into_grid(df):
        # TODO: This Pandas UDF provides incorrect results because it is called before the aggregation is finalized by the watermark.
        if df.empty:
            return None
        row0 = df.iloc[0]
        num_cameras = df.camera.max() + 1
        grid_count = math.ceil(math.sqrt(num_cameras))
        # Determine number of images per row and column.
        image_width = thumbnail_size[0]
        image_height = thumbnail_size[1]
        image_mode = 'RGB'
        margin = 1
        status_width = 0
        # Create blank output image, white background.

        out_pil = Image.new('RGB', ((image_width + margin) * grid_count - margin + status_width, (image_height + margin) * grid_count - margin), (128,128,128))
        # Add images from each camera
        def add_image(r):
            # in_pil = Image.open(io.BytesIO(r['image']))
            in_pil = Image.frombytes(image_mode, (image_width, image_height), r['image'])
            x = (r['camera'] % grid_count) * (image_width + margin)
            y = (r['camera'] // grid_count) * (image_width + margin)
            out_pil.paste(in_pil, (x, y))
        df.apply(add_image, axis=1)

        # font = ImageFont.truetype('/usr/share/fonts/truetype/freefont/FreeSans.ttf', font_size)
        # draw = ImageDraw.Draw(img)
        # draw.text((status_width, 0), 'FRAME\n%05d\nCAMERA\n %03d' % (frame_number, camera), font=font, align='center')

        out_bytesio = io.BytesIO()
        out_pil.save(out_bytesio, format='PNG', compress_level=0)
        out_bytes = out_bytesio.getvalue()

        new_row = pd.Series()
        new_row['timestamp'] = row0['discrete_timestamp']
        new_row['ssrc'] = 0
        new_row['frame_number'] = 0
        new_row['source'] = df[['camera', 'frame_number', 'timestamp']].to_json()
        new_row['data'] = out_bytes
        # new_row['data'] = b''
        return pd.DataFrame([new_row])

    # @pandas_udf(returnType='string', functionType=PandasUDFType.SCALAR)
    # def combine_images_into_grid2(json):
    #     # TODO
    #     def f(data):
    #         in_pil = Image.open(io.BytesIO(data))
    #         out_pil = in_pil.resize(thumbnail_size)
    #         return out_pil.tobytes()
    #     return data_series.apply(f)

    df = grp.apply(combine_images_into_grid)
    df = df.select(func.to_json(func.struct(df["frame_number"], df["data"])).alias("event"))

    # df = grp.agg(func.collect_list('json'))
    # df = df.selectExpr('*', '0 as ssrc')
    # window = Window.partitionBy('ssrc').orderBy('discrete_timestamp').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    # df = df.select('*', func.row_number().over(window))

    # TODO: Output rows are not written in timestamp order. How can this be fixed?
    # Below gives error: Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode
    # df = df.sortWithinPartitions(df['discrete_timestamp'])

    df.printSchema()

    if False:
        (df
         .writeStream
         # .trigger(processingTime='1000 milliseconds')    # limit trigger rate
         .outputMode('append')
         .format('console')
         .option('truncate', 'false')
         .option('checkpointLocation', checkpoint_location)
         .start()
         .awaitTermination()
         )
    else:
        (df
        .writeStream
        .trigger(processingTime="1000 milliseconds")
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
