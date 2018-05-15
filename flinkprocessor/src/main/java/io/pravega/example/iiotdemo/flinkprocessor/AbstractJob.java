package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJob {

    private static Logger log = LoggerFactory.getLogger( AbstractJob.class );

    protected final AppConfiguration appConfiguration;
    protected final FlinkPravegaParams flinkPravegaParams;
    protected final AppConfiguration.PravegaArgs pravegaArgs;

    public AbstractJob(AppConfiguration appConfiguration) {
        this.appConfiguration = appConfiguration;
        this.flinkPravegaParams = appConfiguration.getFlinkPravegaParams();
        this.pravegaArgs = appConfiguration.getPravegaArgs();
    }

    public void createStream(StreamId streamId) {
        StreamManager streamManager = StreamManager.create(flinkPravegaParams.getControllerUri());
        streamManager.createScope(streamId.getScope());
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byDataRate(pravegaArgs.targetRate, pravegaArgs.scaleFactor, pravegaArgs.minNumSegments))
                .build();
        streamManager.createStream(streamId.getScope(), streamId.getName(), streamConfig);
    }

    public void initializeFlinkStreaming() throws Exception {

    }

    public ExecutionEnvironment initializeFlinkBatch() throws Exception {
        // Configure the Flink job environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism, etc.
        int parallelism = appConfiguration.getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        log.info("Parallelism={}", env.getParallelism());
        return env;
    }
}
