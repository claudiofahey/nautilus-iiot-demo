package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.serialization.JsonDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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

        if (appConfiguration.getElasticSearch().isSinkResults()) {
            new ElasticSetup(appConfiguration.getElasticSearch()).run();
        }

        StreamId inputStreamId = pravegaArgs.inputStream;
        log.info("inputStreamId={}", inputStreamId);

        // Configure the Flink job environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism, etc.
        int parallelism = appConfiguration.getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        log.info("Parallelism={}", env.getParallelism());

        final Set<String> streams = new HashSet<>();
        streams.add(inputStreamId.getName());
        DataSet<RawData> events = env.createInput(
                new FlinkPravegaInputFormat<>(
                        flinkPravegaParams.getControllerUri(),
                        inputStreamId.getScope(),
                        streams,
                        new JsonDeserializationSchema<>(RawData.class)),
                TypeInformation.of(RawData.class)
        ).name("EventReader");

        if (appConfiguration.isEnableRebalance()) {
            events = events.rebalance();
            log.info("Rebalancing events");
        }

        events.printToErr();

        // TODO: Perform aggregation.
//                events
//            .groupBy((KeySelector<RawData, String>) data -> data.device_id)
//            .aggregate(new RawDataAggregator.AggregateFunction());

        log.info("Executing {} job", jobName);
        env.execute(jobName);
    }
}
