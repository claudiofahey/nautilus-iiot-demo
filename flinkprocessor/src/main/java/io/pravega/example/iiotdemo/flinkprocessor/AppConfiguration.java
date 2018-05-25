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
    private final ElasticSearch elasticSearch;
    private final String jobClass;
    private final int parallelism;
    private final long checkpointInterval;
    private final boolean disableCheckpoint;
    private final boolean disableOperatorChaining;
    private final boolean enableRebalance;

    public AppConfiguration(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameter Tool: {}", params.toMap());

        pravegaConfig = PravegaConfig.fromParams(params).withDefaultScope("iot");
        inputStreamConfig = new StreamConfig(pravegaConfig,"input-", params);
        outputStreamConfig = new StreamConfig(pravegaConfig,"output-", params);

        jobClass = params.get("jobClass");
        parallelism =params.getInt("parallelism", 1);
        checkpointInterval = params.getLong("checkpointInterval", 10000);     // milliseconds
        disableCheckpoint = params.getBoolean("disableCheckpoint", false);
        disableOperatorChaining = params.getBoolean("disableOperatorChaining", false);
        enableRebalance = params.getBoolean("rebalance", false);

        elasticSearch = new ElasticSearch();
        // elastic-sink: Whether to sink the results to Elastic Search or not.
        elasticSearch.setSinkResults(params.getBoolean("elastic-sink", false));
        elasticSearch.setDeleteIndex(params.getBoolean("elastic-delete-index", false));
        // elastic-host: Host of the Elastic instance to sink to.
        elasticSearch.setHost(params.get("elastic-host", "master.elastic.l4lb.thisdcos.directory"));
        // elastic-port: Port of the Elastic instance to sink to.
        elasticSearch.setPort(params.getInt("elastic-port", 9300));
        // elastic-cluster: The name of the Elastic cluster to sink to.
        elasticSearch.setCluster(params.get("elastic-cluster", "elastic"));
        // elastic-index: The name of the Elastic index to sink to.
        elasticSearch.setIndex(params.get("elastic-index", ""));
        // elastic-type: The name of the type to sink.
        elasticSearch.setType(params.get("elastic-type", ""));
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

    public ElasticSearch getElasticSearch() {
        return elasticSearch;
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

    public static class ElasticSearch implements Serializable {
        private boolean sinkResults;
        private boolean deleteIndex;
        private String host;
        private int port;
        private String cluster;
        private String index;
        private String type;

        public boolean isSinkResults() {
            return sinkResults;
        }

        public void setSinkResults(boolean sinkResults) {
            this.sinkResults = sinkResults;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getCluster() {
            return cluster;
        }

        public void setCluster(String cluster) {
            this.cluster = cluster;
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public boolean isDeleteIndex() {
            return deleteIndex;
        }

        public void setDeleteIndex(boolean deleteIndex) {
            this.deleteIndex = deleteIndex;
        }
    }

}
