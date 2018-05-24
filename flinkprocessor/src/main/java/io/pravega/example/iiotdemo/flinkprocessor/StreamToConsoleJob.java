package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.serialization.JsonDeserializationSchema;
import io.pravega.connectors.flink.serialization.UTF8StringDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamToConsoleJob extends AbstractJob {

    private static Logger log = LoggerFactory.getLogger(StreamToConsoleJob.class);

    public StreamToConsoleJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() throws Exception {
        final String jobName = AppConfiguration.RUN_MODE_STREAM_RAW_DATA;

        StreamExecutionEnvironment env = initializeFlinkStreaming();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        StreamId inputStreamId = pravegaArgs.inputStream;
        log.info("inputStreamId={}", inputStreamId);

        long startTime = 0;
        FlinkPravegaReader<String> flinkPravegaReader = flinkPravegaParams.newReader(
                inputStreamId,
                startTime,
                new UTF8StringDeserializationSchema());

        DataStream<String> ds = env.addSource(flinkPravegaReader);
        ds.printToErr();

        log.info("Executing {} job", jobName);
        env.execute(jobName);
    }
}
