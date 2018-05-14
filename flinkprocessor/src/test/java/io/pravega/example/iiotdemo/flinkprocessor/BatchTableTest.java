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
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP;

public class BatchTableTest {
    private static Logger log = LoggerFactory.getLogger(BatchTableTest.class);
    private static String pravegaControllerURI ="tcp://10.246.21.230:9090";
    private static String pravegaScope = "iot1";

    @Test
    public void BatchTableTest1() throws Exception {
        StreamId inputStreamId = new StreamId(pravegaScope, "data");
        log.info("inputStreamId={}", inputStreamId);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Set<String> streams = new HashSet<>();
        streams.add(inputStreamId.getName());
        DataSet<RawData> rawData = env.createInput(
                new FlinkPravegaInputFormat<>(
                        new URI(pravegaControllerURI),
                        inputStreamId.getScope(),
                        streams,
                        new JsonDeserializationSchema<>(RawData.class)),
                TypeInformation.of(RawData.class)
        );
//        rawData.printToErr();

        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv.registerDataSet("rawData", rawData);
        Table t = tableEnv.scan("rawData");
        t.printSchema();
//        tableEnv.toDataSet(t, Row.class).print();

        t = tableEnv.sqlQuery("select `timestamp`, device_id, vibration1 from rawData");
        tableEnv.toDataSet(t, Row.class).print();
    }

    @Test
    public void BatchTableTest2() throws Exception {
        StreamId inputStreamId = new StreamId(pravegaScope, "data");
        log.info("inputStreamId={}", inputStreamId);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Set<String> streams = new HashSet<>();
        streams.add(inputStreamId.getName());
        DataSet<RawData> rawData = env.createInput(
                new FlinkPravegaInputFormat<>(
                        new URI(pravegaControllerURI),
                        inputStreamId.getScope(),
                        streams,
                        new JsonDeserializationSchema<>(RawData.class)),
                TypeInformation.of(RawData.class)
        );

        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv.registerDataSet("rawData", rawData);
        Table t = tableEnv.scan("rawData");
        t.printSchema();

        tableEnv.registerFunction("TimestampFromLong", new TimestampFromLong());

        t = tableEnv.sqlQuery(
//                "select TimestampFromLong(`timestamp`) as `timestamp`, cast(device_id as varchar) as device_id, temp_celsius, vibration1 from rawData"
                "select TimestampFromLong(`timestamp`) as `timestamp`, device_id, temp_celsius, vibration1 from rawData"
        );
        t.printSchema();
        DataSet<Row> ds = tableEnv.toDataSet(t, Row.class);
//        ds.print();
        tableEnv.registerDataSet("cleanData", ds);

        t = tableEnv.sqlQuery(
//                "select avg(vibration1), tumble_start(`timestamp`, interval '30' second) from cleanData group by tumble(`timestamp`, interval '30' second)"
            "select device_id, tumble_start(`timestamp`, interval '30' second), avg(vibration1) from cleanData group by device_id, tumble(`timestamp`, interval '30' second)"
        );
        t.printSchema();
        ds = tableEnv.toDataSet(t, Row.class);
        ds.print();
    }

    public static class TimestampFromLong extends ScalarFunction {
        public long eval(long t) {
            return t;
        }

        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return TIMESTAMP;
        }
    }
}
