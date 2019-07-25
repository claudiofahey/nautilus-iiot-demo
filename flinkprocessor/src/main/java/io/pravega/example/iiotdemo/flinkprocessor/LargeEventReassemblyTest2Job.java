package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.Pravega;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class LargeEventReassemblyTest2Job extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(LargeEventReassemblyTest2Job.class);

    public LargeEventReassemblyTest2Job(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = LargeEventReassemblyTest2Job.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getInputStreamConfig());

            Schema schema = new Schema()
                    .field("routingKey", Types.STRING())
                    .field("camera", Types.INT())
                    .field("ssrc", Types.INT())
                    .field("timestamp", Types.SQL_TIMESTAMP())
                    .field("frameNumber", Types.INT())
                    .field("data", Types.STRING())
                    .field("chunkIndex", Types.SHORT())
                    .field("finalChunkIndex", Types.SHORT());
            Pravega pravega = new Pravega();
            pravega.tableSourceReaderBuilder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream);
            tableEnv
                    .connect(pravega)
                    .withFormat(new Json().failOnMissingField(false).deriveSchema())
                    .withSchema(schema)
                    .inAppendMode()
                    .registerTableSource("stream1");

            Table t1 = tableEnv.scan("stream1");
            t1.printSchema();

            DataStream<Row> ds1 = tableEnv.toAppendStream(t1, Row.class);
            ds1.printToErr();

            ds1.keyBy(0).printToErr();

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
