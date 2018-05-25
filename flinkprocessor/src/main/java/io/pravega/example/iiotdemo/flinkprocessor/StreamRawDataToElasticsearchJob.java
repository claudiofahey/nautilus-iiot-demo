package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.serialization.JsonDeserializationSchema;
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

public class StreamRawDataToElasticsearchJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(StreamRawDataToElasticsearchJob.class);

    public StreamRawDataToElasticsearchJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = StreamRawDataToElasticsearchJob.class.getName();

            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            createStream(appConfiguration.getInputStreamConfig());

            // TODO: Use table API.

            FlinkPravegaReader<RawData> flinkPravegaReader = FlinkPravegaReader.<RawData>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream)
                    .withDeserializationSchema(new JsonDeserializationSchema<>(RawData.class))
                    .build();

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

            String sqlText = "select * from rawData";
            t = tableEnv.sqlQuery(sqlText);
            t.printSchema();
            DataStream<Row> ds = tableEnv.toAppendStream(t, Row.class);
            ds.printToErr();

            if (appConfiguration.getElasticSearch().isSinkResults()) {
                String index = "iiotdemo-raw-data";
                String type = "record";
                ElasticsearchSetup esSetup = new ElasticsearchSetup(
                        appConfiguration.getElasticSearch().getHost(),
                        appConfiguration.getElasticSearch().getPort(),
                        appConfiguration.getElasticSearch().getCluster(),
                        index,
                        type,
                        appConfiguration.getElasticSearch().isDeleteIndex()
                );
                esSetup.setup();
                ElasticsearchTableSink esSink = new ElasticsearchTableSink(
                        appConfiguration.getElasticSearch().getHost(),
                        appConfiguration.getElasticSearch().getPort(),
                        appConfiguration.getElasticSearch().getCluster(),
                        index,
                        type,
                        null
                );
                t.writeToSink(esSink);
            }

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
