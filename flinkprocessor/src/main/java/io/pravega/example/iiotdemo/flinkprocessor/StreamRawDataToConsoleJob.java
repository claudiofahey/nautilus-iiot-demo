package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaJsonTableSource;
import io.pravega.connectors.flink.FlinkPravegaTableSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamRawDataToConsoleJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(StreamRawDataToConsoleJob.class);

    public StreamRawDataToConsoleJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = StreamRawDataToConsoleJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getInputStreamConfig());
            TableSchema rawDataSchema = TableSchema.fromTypeInfo(TypeInformation.of(RawData.class));
            // TODO: How do I set event time?
            FlinkPravegaTableSource rawDataTableSource = FlinkPravegaJsonTableSource.builder()
                .withPravegaConfig(appConfiguration.getPravegaConfig())
                .forStream(appConfiguration.getInputStreamConfig().stream)
                .withSchema(rawDataSchema)
                .build();
            tableEnv.registerTableSource("rawData", rawDataTableSource);
            Table t = tableEnv.scan("rawData");
            t.printSchema();
            tableEnv.toAppendStream(t, Row.class).printToErr();
            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
