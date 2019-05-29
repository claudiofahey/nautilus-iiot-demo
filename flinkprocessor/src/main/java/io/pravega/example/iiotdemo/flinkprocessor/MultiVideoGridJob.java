package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaJsonTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiVideoGridJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MultiVideoGridJob.class);

    public MultiVideoGridJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = MultiVideoGridJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getInputStreamConfig());
            // Define the input schema.
            TableSchema inputSchema = TableSchema.builder()
                    .field("timestamp", Types.SQL_TIMESTAMP())
                    .field("frame_number", Types.INT())
                    .field("camera", Types.INT())
                    .field("ssrc", Types.INT())
                    .field("data", Types.PRIMITIVE_ARRAY(Types.BYTE()))
                    .build();
            FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                    .forStream(appConfiguration.getInputStreamConfig().stream)
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .failOnMissingField(true)
                    .withRowtimeAttribute("timestamp", new ExistingField("timestamp"), new BoundedOutOfOrderTimestamps(1000L))
                    .withSchema(inputSchema)
                    .build();
            tableEnv.registerTableSource("video", source);
            Table t = tableEnv.scan("video");
            t.printSchema();
            tableEnv.toAppendStream(t, Row.class).printToErr();
            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
