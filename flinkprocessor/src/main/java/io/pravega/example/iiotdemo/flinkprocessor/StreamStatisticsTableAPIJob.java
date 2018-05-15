package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.serialization.JsonDeserializationSchema;
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

// Uses the Flink table API to calculate streaming statistics.
public class StreamStatisticsTableAPIJob extends AbstractJob {

    private static Logger log = LoggerFactory.getLogger(StreamStatisticsTableAPIJob.class);

    public StreamStatisticsTableAPIJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() throws Exception {
        final String jobName = AppConfiguration.RUN_MODE_STREAM_STATISTICS_TABLE_API;

        StreamExecutionEnvironment env = initializeFlinkStreaming();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        StreamId inputStreamId = pravegaArgs.inputStream;
        log.info("inputStreamId={}", inputStreamId);

        long startTime = 0;
        FlinkPravegaReader<RawData> flinkPravegaReader = flinkPravegaParams.newReader(
                inputStreamId,
                startTime,
                new JsonDeserializationSchema<>(RawData.class));

        DataStream<RawData> rawData = env
                .addSource(flinkPravegaReader)
                .name("EventReader");

        rawData = rawData.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<RawData>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(RawData element) {
                return element.timestamp;
            }
        });

        tableEnv.registerDataStream("rawData", rawData, "event_type,device_id,temp_celsius,vibration1,vibration2,timestamp.rowtime");
        Table t = tableEnv.scan("rawData");
        t.printSchema();

        String sqlText =
            "select\n" +
            "  vib.device_id, tumble_end(vib.`timestamp`, interval '10' second) as `timestamp`,\n" +
            "  avg(vib.vibration1) as vibration1, avg(temp.temp_celsius) as temp_celsius\n" +
            "from\n" +
            "  (select device_id, `timestamp`, vibration1 from rawData where event_type='vibration') vib,\n" +
            "  (select device_id, `timestamp`, temp_celsius from rawData where event_type='temp') temp\n" +
            "where\n" +
            "    vib.device_id = temp.device_id and\n" +
            "    vib.`timestamp` between temp.`timestamp` - interval '3' second and temp.`timestamp` + interval '3' second\n" +
            "group by vib.device_id, tumble(vib.`timestamp`, interval '10' second)";

        log.info("sqlText=\n{}", sqlText);
        t = tableEnv.sqlQuery(sqlText);
        t.printSchema();
        DataStream<Row> ds = tableEnv.toAppendStream(t, Row.class);
        ds.printToErr();

        if (appConfiguration.getElasticSearch().isSinkResults()) {
            ElasticsearchStreamSink esSink = new ElasticsearchStreamSink(
                    appConfiguration.getElasticSearch().getHost(),
                    appConfiguration.getElasticSearch().getPort(),
                    appConfiguration.getElasticSearch().getCluster(),
                    "iiotdemo-stream-statistics-table-api",
                    "record",
                    false,
                    -1,
                    t
            );
            esSink.setup();
            esSink.addSink().name("Write to ElasticSearch");
        }

        log.info("Executing {} job", jobName);
        env.execute(jobName);
    }
}
