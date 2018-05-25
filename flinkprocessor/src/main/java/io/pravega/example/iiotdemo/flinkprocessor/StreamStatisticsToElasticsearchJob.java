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

// Uses the Flink table API to calculate streaming statistics.
public class StreamStatisticsToElasticsearchJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(StreamStatisticsToElasticsearchJob.class);

    public StreamStatisticsToElasticsearchJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = StreamStatisticsToElasticsearchJob.class.getName();

            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            createStream(appConfiguration.getInputStreamConfig());
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

            String sqlText =
                    "select\n" +
                            "  date_format(tumble_end(vib.`timestamp`, interval '10' second), '%Y%d%m%H%i%s') || '-' || vib.device_id as id,\n" +
                            "  vib.device_id,\n" +
                            "  tumble_end(vib.`timestamp`, interval '10' second) as `timestamp`,\n" +
                            "  avg(vib.vibration1) as vibration1,\n" +
                            "  avg(vib.vibration2) as vibration2,\n" +
                            "  avg(temp.temp_celsius) as temp_celsius\n" +
                            "from\n" +
                            "  (select device_id, `timestamp`, vibration1, vibration2 from rawData where event_type='vibration') vib,\n" +
                            "  (select device_id, `timestamp`, temp_celsius from rawData where event_type='temp') temp\n" +
                            "where\n" +
                            "    vib.device_id = temp.device_id and\n" +
                            "    vib.`timestamp` between temp.`timestamp` - interval '3' second and temp.`timestamp` + interval '3' second\n" +
                            "group by\n" +
                            "  vib.device_id,\n" +
                            "  tumble(vib.`timestamp`, interval '10' second)";

            log.info("sqlText=\n{}", sqlText);
            t = tableEnv.sqlQuery(sqlText);
            t.printSchema();
            DataStream<Row> ds = tableEnv.toAppendStream(t, Row.class);
            ds.printToErr();

            if (appConfiguration.getElasticSearch().isSinkResults()) {
                String index = "iiotdemo-statistics";
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
                        "id"
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