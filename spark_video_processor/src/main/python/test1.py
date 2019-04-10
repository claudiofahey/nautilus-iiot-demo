from pyspark.sql import SparkSession
import os

spark = (SparkSession
    .builder
    .appName('test1')
    .getOrCreate()
    )

#controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
#scope = os.getenv('PRAVEGA_SCOPE', 'examples')

df = (spark
    .readStream
    .json('data/1', schema='time int, camera int, chunk int, data double')
    )

# group by (camera, chunk), aggregate data
(df
    .groupBy('chunk')
    .sum('data')
    )

(df
    .writeStream
    .trigger(processingTime='3 seconds')
    .outputMode('append')
    .format('console')
    .start()
    .awaitTermination()
    )
