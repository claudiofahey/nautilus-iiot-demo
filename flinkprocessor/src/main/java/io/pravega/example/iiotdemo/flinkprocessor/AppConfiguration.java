package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class AppConfiguration {
    private static Logger log = LoggerFactory.getLogger(AppConfiguration.class);

    private final PravegaConfig pravegaConfig;
    private final StreamConfig inputStreamConfig;
    private final StreamConfig outputStreamConfig;
    private final String jobClass;
    private final int parallelism;
    private final long checkpointInterval;
    private final boolean disableCheckpoint;
    private final boolean disableOperatorChaining;
    private final boolean enableRebalance;

    public AppConfiguration(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameter Tool: {}", params.toMap());

        pravegaConfig = PravegaConfig.fromParams(params).withDefaultScope("examples"); // TODO: make configurable
        inputStreamConfig = new StreamConfig(pravegaConfig,"input-", params);
        outputStreamConfig = new StreamConfig(pravegaConfig,"output-", params);

        jobClass = params.get("jobClass");
        parallelism =params.getInt("parallelism", 1);
        checkpointInterval = params.getLong("checkpointInterval", 10000);     // milliseconds
        disableCheckpoint = params.getBoolean("disableCheckpoint", false);
        disableOperatorChaining = params.getBoolean("disableOperatorChaining", false);
        enableRebalance = params.getBoolean("rebalance", false);
    }

    public PravegaConfig getPravegaConfig() {
        return pravegaConfig;
    }

    public StreamConfig getInputStreamConfig() {
        return inputStreamConfig;
    }

    public StreamConfig getOutputStreamConfig() {
        return outputStreamConfig;
    }

    public String getJobClass() {
        return jobClass;
    }

    public int getParallelism() {
        return parallelism;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public boolean isDisableCheckpoint() {
        return disableCheckpoint;
    }

    public boolean isDisableOperatorChaining() {
        return disableOperatorChaining;
    }

    public boolean isEnableRebalance() {
        return enableRebalance;
    }

    public static class StreamConfig {
        protected Stream stream;
        protected int targetRate;
        protected int scaleFactor;
        protected int minNumSegments;

        public StreamConfig(PravegaConfig pravegaConfig, String argPrefix, ParameterTool params) {
            stream = pravegaConfig.resolve(params.get(argPrefix + "stream", "default"));
            targetRate = params.getInt(argPrefix + "targetRate", 100000);  // Data rate in KB/sec
            scaleFactor = params.getInt(argPrefix + "scaleFactor", 2);
            minNumSegments = params.getInt(argPrefix + "minNumSegments", 12);
        }
    }
}
