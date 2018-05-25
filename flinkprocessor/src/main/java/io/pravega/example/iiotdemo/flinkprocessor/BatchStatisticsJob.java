package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.FlinkPravegaJsonTableSource;
import io.pravega.connectors.flink.FlinkPravegaTableSource;
import io.pravega.connectors.flink.serialization.JsonDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchStatisticsJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(BatchStatisticsJob.class);

    public BatchStatisticsJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = BatchStatisticsJob.class.getName();

            ExecutionEnvironment env = initializeFlinkBatch();
            BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            createStream(appConfiguration.getInputStreamConfig());

            boolean useTableSource = false;
            if (useTableSource) {
                // TODO: This doesn't work.
                TableSchema rawDataSchema = TableSchema.fromTypeInfo(TypeInformation.of(RawData.class));
                FlinkPravegaTableSource rawDataTableSource = FlinkPravegaJsonTableSource.builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream)
                    .withSchema(rawDataSchema)
                    .build();
                tableEnv.registerTableSource("rawData", rawDataTableSource);
            } else {
                DataSet<RawData> rawDataDataSet = env.createInput(
                    FlinkPravegaInputFormat.<RawData>builder()
                        .withPravegaConfig(appConfiguration.getPravegaConfig())
                        .forStream(appConfiguration.getInputStreamConfig().stream)
                        .withDeserializationSchema(new JsonDeserializationSchema<>(RawData.class))
                        .build(),
                    TypeInformation.of(RawData.class));
                tableEnv.registerDataSet("rawData", rawDataDataSet);
            }

            Table rawDataTable = tableEnv.scan("rawData");
            rawDataTable.printSchema();

            tableEnv.registerFunction("TimestampFromLong", new TimestampFromLong());

            Table cleanDataTable = tableEnv.sqlQuery(
                "select TimestampFromLong(`timestamp`) as `timestamp`, device_id, temp_celsius, vibration1 from rawData"
            );
            tableEnv.registerTable("cleanData", cleanDataTable);

            String sqlText =
                "select device_id, tumble_start(`timestamp`, interval '30' second), avg(vibration1)\n" +
                "from cleanData group by device_id, tumble(`timestamp`, interval '30' second)";
            Table statsTable = tableEnv.sqlQuery(sqlText);
            statsTable.printSchema();
            tableEnv.toDataSet(statsTable, Row.class).printOnTaskManager("STATISTICS");

            // TODO: Output to a database.

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
