package io.pravega.example.iiotdemo.sparkprocessor;

import io.pravega.connectors.hadoop.EventKey;
import io.pravega.connectors.hadoop.PravegaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkPravegaRDDBatchTest {
    private static Logger log = LoggerFactory.getLogger(SparkPravegaRDDBatchTest.class);
    private static String pravegaControllerURI ="tcp://10.246.21.230:9090";
    private static String pravegaScope = "iot1";

    @Test
    public void Test1() throws Exception {
        String pravegaStream = "data";
        SparkSession spark = SparkSession.builder().appName("Test1").master("local[2]").getOrCreate();
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
    public void Test2() throws Exception {
        String pravegaStream = "data";
        String startPos = "[[{\"scope\":\"iot1\",\"streamName\":\"data\",\"segmentNumber\":0},2196578]]";
//        startPos = "";
        String endPos = PravegaInputFormat.fetchLatestPositionsJson(pravegaControllerURI, pravegaScope, pravegaStream);
        log.info("startPos={}, endPos={}", startPos, endPos);

        SparkSession spark = SparkSession.builder().appName("Test1").master("local[2]").getOrCreate();
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
}
