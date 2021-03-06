package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJob implements Runnable {
    private static Logger log = LoggerFactory.getLogger(AbstractJob.class);

    protected final AppConfiguration appConfiguration;

    public AbstractJob(AppConfiguration appConfiguration) {
        this.appConfiguration = appConfiguration;
    }

    public void createStream(AppConfiguration.StreamConfig streamConfig) {
        try(StreamManager streamManager = StreamManager.create(appConfiguration.getPravegaConfig().getClientConfig())) {
            // create the requested scope (if necessary)
//            streamManager.createScope(streamConfig.stream.getScope());
            // create the requested stream
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byDataRate(streamConfig.targetRate, streamConfig.scaleFactor, streamConfig.minNumSegments))
                    .build();
            streamManager.createStream(streamConfig.stream.getScope(), streamConfig.stream.getStreamName(), streamConfiguration);
        }
    }

    public StreamInfo getStreamInfo(Stream stream) {
        try(StreamManager streamManager = StreamManager.create(appConfiguration.getPravegaConfig().getClientConfig())) {
            return streamManager.getStreamInfo(stream.getScope(), stream.getStreamName());
        }
    }

    public StreamExecutionEnvironment initializeFlinkStreaming() throws Exception {
        // Configure the Flink job environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set parallelism, etc.
        int parallelism = appConfiguration.getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        if (appConfiguration.isDisableOperatorChaining()) {
            env.disableOperatorChaining();
        }
        if(!appConfiguration.isDisableCheckpoint()) {
            long checkpointInterval = appConfiguration.getCheckpointInterval();
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        }
        log.info("Parallelism={}, MaxParallelism={}", env.getParallelism(), env.getMaxParallelism());
        // We can't use MemoryStateBackend because it can't store our large state.
        if (env.getStateBackend() == null || env.getStateBackend() instanceof MemoryStateBackend) {
            log.warn("Using FsStateBackend");
            env.setStateBackend(new FsStateBackend("file:///tmp/flink-state", true));
        }
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
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
