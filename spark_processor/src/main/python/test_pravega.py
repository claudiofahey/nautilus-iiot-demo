from pyspark.sql import SparkSession
import os
import sys


def main():
    print('THIS IS PYTHON VERSION %s' % sys.version)
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
    Test of Spark SQL batch mode.
    """
    controller = os.getenv('PRAVEGA_CONTROLLER', 'tcp://127.0.0.1:9090')
    scope = os.getenv('PRAVEGA_SCOPE', 'examples')
    df = (spark
          .read
          .format("pravega")
          .option("controller", controller)
          .option("scope", scope)
          .option("stream", "sensors")
          .load()
          )
    # df.show()
    print('NUMBER OF RECORDS: %d' % df.count())


if __name__ == '__main__':
    main()
