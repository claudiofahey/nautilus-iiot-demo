package io.pravega.example.iiotdemo.flinkprocessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.serialization.JsonDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Uses the low-level Flink API to calculate streaming statistics.
public class StreamStatisticsTableAPIJob extends AbstractJob {

    private static Logger log = LoggerFactory.getLogger(StreamStatisticsTableAPIJob.class);

    public StreamStatisticsTableAPIJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() throws Exception {
        final String jobName = AppConfiguration.RUN_MODE_STREAM_STATISTICS_TABLE_API;

        if (appConfiguration.getElasticSearch().isSinkResults()) {
            new ElasticSetup(appConfiguration.getElasticSearch()).run();
        }

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
//        DataStream<Row> ds = tableEnv.toAppendStream(t, Row.class);
//        ds.printToErr();

        String sqlText =
            "select\n" +
            "  vib.device_id, tumble_start(vib.`timestamp`, interval '10' second) as timestamp_start,\n" +
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

//        if (appConfiguration.getElasticSearch().isSinkResults()) {
//            ElasticsearchStreamSink<StatisticsAggregator.Result> elasticSink = sinkToElasticSearch();
//            aggEvents2.addSink(elasticSink).name("Write to ElasticSearch");
//        }

        log.info("Executing {} job", jobName);
        env.execute(jobName);
    }

    private ElasticsearchSink sinkToElasticSearch() throws Exception {
        String host = appConfiguration.getElasticSearch().getHost();
        int port = appConfiguration.getElasticSearch().getPort();
        String cluster = appConfiguration.getElasticSearch().getCluster();
        String index = appConfiguration.getElasticSearch().getIndex();
        String type = appConfiguration.getElasticSearch().getType();

        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", cluster);
        config.put("client.transport.sniff", "false");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName(host), port));

        return new ElasticsearchSink(config, transports, new ResultSinkFunction(index, type, appConfiguration.getElasticSearch()));
    }

    public static class ResultSinkFunction implements ElasticsearchSinkFunction<StatisticsAggregator.Result> {
        private static final Logger LOG = LoggerFactory.getLogger(ResultSinkFunction.class);

        private final String index;
        private final String type;
        private final ObjectMapper objectMapper = new ObjectMapper();

        public ResultSinkFunction(String index, String type, AppConfiguration.ElasticSearch elasticConfig) {
            this.index = index;
            this.type = type;
        }

        @Override
        public void process(StatisticsAggregator.Result event, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(event));
        }

        private IndexRequest createIndexRequest(StatisticsAggregator.Result event) {
            String source = null;
            try {
                source = objectMapper.writeValueAsString(event);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
            String id = String.format("%d", event.timestamp);
            return Requests.indexRequest()
                .index(index)
                .type(type)
                .id(id)
                .source(source);
        }
    }
}
