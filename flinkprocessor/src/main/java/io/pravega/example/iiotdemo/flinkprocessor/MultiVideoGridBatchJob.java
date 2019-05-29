package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaJsonTableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiVideoGridBatchJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MultiVideoGridBatchJob.class);

    public MultiVideoGridBatchJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = MultiVideoGridBatchJob.class.getName();
            ExecutionEnvironment env = initializeFlinkBatch();
            BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getInputStreamConfig());
            // Define the input schema.
            TableSchema inputSchema = TableSchema.builder()
                    .field("timestamp", Types.SQL_TIMESTAMP())
                    .field("frame_number", Types.INT())
                    .field("camera", Types.INT())
                    .field("ssrc", Types.INT())
//                    .field("data", Types.STRING())
                    .build();
            FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                    .forStream(appConfiguration.getInputStreamConfig().stream)
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .failOnMissingField(true)
//                    .withRowtimeAttribute("timestamp", new ExistingField("timestamp"), new BoundedOutOfOrderTimestamps(1000L))
                    .withSchema(inputSchema)
                    .build();
            tableEnv.registerTableSource("video", source);
            Table t = tableEnv.scan("video");
            t.printSchema();
            DataSet<Row> ds = tableEnv.toDataSet(t, Row.class);
            long numRows = ds.count();
            log.info("numRows={}", numRows);
            tableEnv.toDataSet(t, Row.class).printToErr();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
