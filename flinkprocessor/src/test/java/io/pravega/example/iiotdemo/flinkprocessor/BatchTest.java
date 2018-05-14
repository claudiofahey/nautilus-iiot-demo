package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.serialization.UTF8StringDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class BatchTest {
    private static Logger log = LoggerFactory.getLogger(BatchTest.class);
    private static String pravegaControllerURI ="tcp://10.246.21.230:9090";
    private static String pravegaScope = "iot1";

    @Test
    public void BatchTest1() throws Exception {
        // TODO: Change to use new API being developed in https://github.com/pravega/flink-connectors/issues/62.
        StreamId inputStreamId = new StreamId(pravegaScope, "data");
        log.info("inputStreamId={}", inputStreamId);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Set<String> streams = new HashSet<>();
        streams.add(inputStreamId.getName());
        DataSet<String> strings = env.createInput(
                new FlinkPravegaInputFormat<>(
                        new URI(pravegaControllerURI),
                        inputStreamId.getScope(),
                        streams,
                        new UTF8StringDeserializationSchema()),
                BasicTypeInfo.STRING_TYPE_INFO
        );
        strings.printToErr();
    }
}
