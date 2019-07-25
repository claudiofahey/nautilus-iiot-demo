package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.Pravega;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class LargeEventWriterTestJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(LargeEventWriterTestJob.class);

    public LargeEventWriterTestJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = LargeEventWriterTestJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getOutputStreamConfig());

            DataStream<Integer> ds1 = env.fromElements(0, 1, 2, 3);
            DataStream<Tuple8<String, Integer, Integer, Timestamp, Integer, byte[], Short, Short>> ds2 =
                    ds1.flatMap(new FlatMapFunction<Integer, Tuple8<String, Integer, Integer, Timestamp, Integer, byte[], Short, Short>>() {
                @Override
                public void flatMap(Integer frameNumber, Collector<Tuple8<String, Integer, Integer, Timestamp, Integer, byte[], Short, Short>> out) throws Exception {
                    Integer camera = 0;
                    Integer ssrc = 0;
                    String routingKey = String.format("%d-%d", camera, ssrc);
                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                    out.collect(new Tuple8<>(routingKey, camera, ssrc, timestamp, frameNumber, new byte[]{0, 1}, (short) 0, (short) 1));
                    out.collect(new Tuple8<>(routingKey, camera, ssrc, timestamp, frameNumber, new byte[]{2, 3}, (short) 1, (short) 1));
                }
            });
//            ds2.printToErr();

            Table t1 = tableEnv.fromDataStream(ds2, "routingKey, camera, ssrc, timestamp, frameNumber, data, chunkIndex, finalChunkIndex");
            tableEnv.toAppendStream(t1, Row.class).printToErr();

            Schema schema = new Schema()
                    .field("routingKey", Types.STRING())
                    .field("camera", Types.INT())
                    .field("ssrc", Types.INT())
                    .field("timestamp", Types.SQL_TIMESTAMP())
                    .field("frameNumber", Types.INT())
                    .field("data", Types.PRIMITIVE_ARRAY(Types.BYTE()))
                    .field("chunkIndex", Types.SHORT())
                    .field("finalChunkIndex", Types.SHORT());
            Pravega pravega = new Pravega();
            pravega.tableSinkWriterBuilder()
                    .withRoutingKeyField("routingKey")
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getOutputStreamConfig().stream);
            tableEnv
                    .connect(pravega)
                    .withFormat(new Json().failOnMissingField(false).deriveSchema())
                    .withSchema(schema)
                    .inAppendMode()
                    .registerTableSink("stream1");
            t1.insertInto("stream1");

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
