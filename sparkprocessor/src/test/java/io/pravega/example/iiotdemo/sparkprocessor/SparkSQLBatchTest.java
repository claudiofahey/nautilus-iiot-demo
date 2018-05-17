package io.pravega.example.iiotdemo.sparkprocessor;

import io.pravega.connectors.hadoop.EventKey;
import io.pravega.connectors.hadoop.PravegaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import org.apache.spark.sql.types.DataType;

import java.util.ArrayList;
import java.util.List;

public class SparkSQLBatchTest {
    private static Logger log = LoggerFactory.getLogger(SparkSQLBatchTest.class);

    @Test
    public void BatchTest1() throws Exception {
        SparkSession spark = SparkSession.builder().appName("BatchTest1").master("local[2]").getOrCreate();
        SparkContext sc = spark.sparkContext();
//        Dataset<Row> df = spark.read().json("/tmp/test.json");
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("key1", "value1"));
        rows.add(RowFactory.create("key2", "value2"));
        StructType schema = new StructType()
            .add("key","string")
            .add("value","string");
        log.info("schema={}", schema);
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.printSchema();
        df.show();
        spark.stop();

    }
}
