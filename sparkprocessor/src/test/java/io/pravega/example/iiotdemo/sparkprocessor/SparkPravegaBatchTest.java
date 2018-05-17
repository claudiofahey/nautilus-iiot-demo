package io.pravega.example.iiotdemo.sparkprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.connectors.hadoop.EventKey;
import io.pravega.connectors.hadoop.PravegaInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import scala.Tuple2;

public class SparkPravegaBatchTest {
    private static Logger log = LoggerFactory.getLogger(SparkPravegaBatchTest.class);
    private static String pravegaControllerURI ="tcp://10.246.21.230:9090";
    private static String pravegaScope = "iot1";

    @Test
    public void BatchTest1() throws Exception {
        String pravegaStream = "data";
        SparkSession spark = SparkSession.builder().appName("BatchTest1").master("local[2]").getOrCreate();
        SparkContext sc = spark.sparkContext();
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, pravegaScope);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, pravegaStream);
        conf.setStrings(PravegaInputFormat.URI_STRING, pravegaControllerURI);
        conf.setStrings(PravegaInputFormat.DESERIALIZER, RawDataSerializer.class.getName());
        RDD<Tuple2<EventKey, RawData>> rdd = sc.newAPIHadoopRDD(conf, PravegaInputFormat.class, EventKey.class, RawData.class);
        log.info("rdd={}", rdd.collect());
        spark.stop();
    }

    @Test
    public void BatchTest2() throws Exception {
        String pravegaStream = "data";
        String startPos = "[[{\"scope\":\"iot1\",\"streamName\":\"data\",\"segmentNumber\":0},2196578]]";
//        startPos = "";
        String endPos = PravegaInputFormat.fetchLatestPositionsJson(pravegaControllerURI, pravegaScope, pravegaStream);
        log.info("startPos={}, endPos={}", startPos, endPos);

        SparkSession spark = SparkSession.builder().appName("BatchTest1").master("local[2]").getOrCreate();
        SparkContext sc = spark.sparkContext();

        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, pravegaScope);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, pravegaStream);
        conf.setStrings(PravegaInputFormat.URI_STRING, pravegaControllerURI);
        conf.setStrings(PravegaInputFormat.DESERIALIZER, RawDataSerializer.class.getName());
        conf.setStrings(PravegaInputFormat.START_POSITIONS, startPos);
        conf.setStrings(PravegaInputFormat.END_POSITIONS, endPos);

        RDD<Tuple2<EventKey, RawData>> rdd = sc.newAPIHadoopRDD(conf, PravegaInputFormat.class, EventKey.class, RawData.class);
//        log.info("rdd={}", rdd.collect());
        long rddCount = rdd.count();

        spark.stop();
        log.info("rdd.count={}", rddCount);
    }

    @Test
    public void BatchTest3() throws Exception {
        String pravegaStream = "data";
//        String startPos = "[[{\"scope\":\"iot1\",\"streamName\":\"data\",\"segmentNumber\":0},2196578]]";
        String startPos = "";
        String endPos = PravegaInputFormat.fetchLatestPositionsJson(pravegaControllerURI, pravegaScope, pravegaStream);
        log.info("startPos={}, endPos={}", startPos, endPos);

        SparkSession spark = SparkSession.builder().appName("BatchTest1").master("local[2]").getOrCreate();
        SparkContext sc = spark.sparkContext();

        StructType schema = new StructType()
                .add("timestamp","string")
                .add("event_type","string");
        String schemaText = schema.json();
        log.info("schemaText={}", schemaText);

        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, pravegaScope);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, pravegaStream);
        conf.setStrings(PravegaInputFormat.URI_STRING, pravegaControllerURI);
        conf.setStrings(PravegaInputFormat.DESERIALIZER, JsonRowSerializer.class.getName());
        conf.setStrings(PravegaInputFormat.START_POSITIONS, startPos);
        conf.setStrings(PravegaInputFormat.END_POSITIONS, endPos);
        conf.setStrings("schema", schemaText);

        RDD<Tuple2<EventKey, Row>> rdd = sc.newAPIHadoopRDD(conf, PravegaInputFormat.class, EventKey.class, Row.class);
        log.info("rdd={}", rdd.collect());
        long rddCount = rdd.count();
        log.info("rdd.count={}", rddCount);

//        rdd.to
//        RDD<Row> rows = rdd.values();
        JavaRDD<Tuple2<EventKey, Row>> jrdd = rdd.toJavaRDD();
        JavaPairRDD<EventKey, Row> pairs = JavaPairRDD.fromJavaRDD(jrdd);
        RDD<Row> rows = pairs.values().rdd();
//        RDD<Row> rows = rdd.map((Tuple2<EventKey, Row> t) -> {
//            return t._2();
//        });

//        StructType schema = new StructType()
//                .add("timestamp","string")
//                .add("event_type","string");
//        log.info("schema={}", schema);
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.printSchema();
        df.show();

        spark.stop();
    }
}
