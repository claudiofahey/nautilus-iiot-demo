package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.serialization.JsonDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class BatchStatisticsJob extends AbstractJob {

    private static Logger log = LoggerFactory.getLogger(BatchStatisticsJob.class);

    public BatchStatisticsJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() throws Exception {
        final String jobName = AppConfiguration.RUN_MODE_BATCH_STATISTICS;

        ExecutionEnvironment env = initializeFlinkBatch();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        StreamId inputStreamId = pravegaArgs.inputStream;
        log.info("inputStreamId={}", inputStreamId);

        final Set<String> streams = new HashSet<>();
        streams.add(inputStreamId.getName());
        DataSet<RawData> rawData = env.createInput(
                new FlinkPravegaInputFormat<>(
                        flinkPravegaParams.getControllerUri(),
                        inputStreamId.getScope(),
                        streams,
                        new JsonDeserializationSchema<>(RawData.class)),
                TypeInformation.of(RawData.class)
        ).name("EventReader");

        tableEnv.registerDataSet("rawData", rawData);
        Table t = tableEnv.scan("rawData");
        t.printSchema();

        tableEnv.registerFunction("TimestampFromLong", new TimestampFromLong());

        t = tableEnv.sqlQuery(
                "select TimestampFromLong(`timestamp`) as `timestamp`, device_id, temp_celsius, vibration1 from rawData"
        );
        t.printSchema();
        DataSet<Row> ds = tableEnv.toDataSet(t, Row.class);
        tableEnv.registerDataSet("cleanData", ds);

        String sqlText =
            "select device_id, tumble_start(`timestamp`, interval '30' second), avg(vibration1)\n" +
            "from cleanData group by device_id, tumble(`timestamp`, interval '30' second)";
        t = tableEnv.sqlQuery(sqlText);
        t.printSchema();
        ds = tableEnv.toDataSet(t, Row.class);
        ds.printOnTaskManager("STATISTICS");

        // TODO: Output to a database.

        log.info("Executing {} job", jobName);
        env.execute(jobName);
    }
}
