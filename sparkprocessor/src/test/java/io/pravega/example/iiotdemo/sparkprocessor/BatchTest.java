package io.pravega.example.iiotdemo.sparkprocessor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class BatchTest {
    private static Logger log = LoggerFactory.getLogger(BatchTest.class);
    private static String pravegaControllerURI ="tcp://10.246.21.230:9090";
    private static String pravegaScope = "iot1";

    @Test
    public void BatchTest1() throws Exception {
        log.info("BatchTest1");

        String logFile = "/tmp/README.md";
        SparkSession spark = SparkSession.builder().appName("BatchTest1").master("local[1]").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
