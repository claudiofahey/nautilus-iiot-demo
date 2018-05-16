package io.pravega.example.iiotdemo.sparkprocessor;

import io.pravega.connectors.hadoop.EventKey;
import io.pravega.connectors.hadoop.PravegaInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
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
        SparkSession spark = SparkSession.builder().appName("BatchTest1").master("local[2]").getOrCreate();
        SparkContext sc = spark.sparkContext();

        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, pravegaScope);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, "data");
        conf.setStrings(PravegaInputFormat.URI_STRING, pravegaControllerURI);
        conf.setStrings(PravegaInputFormat.DESERIALIZER, RawDataSerializer.class.getName());

        RDD<Tuple2<EventKey, RawData>> rdd = sc.newAPIHadoopRDD(conf, PravegaInputFormat.class, EventKey.class, RawData.class);
        log.info("rdd={}", rdd.collect());

        spark.stop();
    }
}
