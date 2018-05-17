package io.pravega.example.iiotdemo.sparkprocessor;

import io.pravega.connectors.hadoop.EventKey;
import io.pravega.connectors.hadoop.PravegaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

import java.util.ArrayList;
import java.util.List;

public class SparkPravegaSQLBatchTest {
    private static Logger log = LoggerFactory.getLogger(SparkPravegaSQLBatchTest.class);
    private static String pravegaControllerURI ="tcp://10.246.21.230:9090";
    private static String pravegaScope = "iot1";

    @Test
    public void Test1() throws Exception {
        SparkSession spark = SparkSession.builder().appName("Test1").master("local[2]").getOrCreate();
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

    @Test
    public void Test2() throws Exception {
        String pravegaStream = "data";
//        String startPos = "[[{\"scope\":\"iot1\",\"streamName\":\"data\",\"segmentNumber\":0},2196578]]";
        String startPos = "";
        String endPos = PravegaInputFormat.fetchLatestPositionsJson(pravegaControllerURI, pravegaScope, pravegaStream);
        log.info("startPos={}, endPos={}", startPos, endPos);

        SparkSession spark = SparkSession.builder().appName("Test1").master("local[2]").getOrCreate();
        SparkContext sc = spark.sparkContext();

        StructType schema = new StructType()
                .add("timestamp","string")
                .add("event_type","string");
        String schemaText = schema.json();
        log.info("schemaText={}", schemaText);
        schema = null;

        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, pravegaScope);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, pravegaStream);
        conf.setStrings(PravegaInputFormat.URI_STRING, pravegaControllerURI);
        conf.setStrings(PravegaInputFormat.DESERIALIZER, UTF8StringSerializer.class.getName());
        conf.setStrings(PravegaInputFormat.START_POSITIONS, startPos);
        conf.setStrings(PravegaInputFormat.END_POSITIONS, endPos);
        RDD<Tuple2<EventKey, String>> rdd = sc.newAPIHadoopRDD(conf, PravegaInputFormat.class, EventKey.class, String.class);
        JavaRDD<Tuple2<EventKey, String>> jrdd = rdd.toJavaRDD();
        JavaPairRDD<EventKey, String> pairs = JavaPairRDD.fromJavaRDD(jrdd);
        RDD<String> strings = pairs.values().rdd();
        Dataset<Row> df = spark.read().schema(schema).json(strings);

        df.printSchema();
        df.show();

        spark.stop();
    }
}
