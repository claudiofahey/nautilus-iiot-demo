package io.pravega.example.iiotdemo.flinkprocessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.types.Row;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Based on https://github.com/apache/flink/blob/master/flink-connectors/flink-jdbc/src/main/java/org/apache/flink/api/java/io/jdbc/JDBCAppendTableSink.java.
public class ElasticsearchTableSink implements AppendStreamTableSink<Row>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchTableSink.class);

    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private String idFieldName;
    private String host;
    private int port;
    private String cluster;
    private String index;
    private String type;
    private boolean isDeleteIndex;

    public ElasticsearchTableSink() {
    }

    public ElasticsearchTableSink(String idFieldName, String host, int port, String cluster, String index, String type, boolean isDeleteIndex) {
        this.idFieldName = idFieldName;
        this.host = host;
        this.port = port;
        this.cluster = cluster;
        this.index = index;
        this.type = type;
        this.isDeleteIndex = isDeleteIndex;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public ElasticsearchTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        ElasticsearchTableSink copy = new ElasticsearchTableSink();
        copy.fieldNames = fieldNames;
        copy.fieldTypes = fieldTypes;
        copy.idFieldName = idFieldName;
        copy.host = host;
        copy.port = port;
        copy.cluster = cluster;
        copy.index = index;
        copy.type = type;
        copy.isDeleteIndex = isDeleteIndex;
        return copy;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        LOG.info("emitDataStream: dataStream={}", dataStream);
        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", cluster);
        config.put("client.transport.sniff", "false");
        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(host, port));
        ElasticsearchSink<Row> sink = new ElasticsearchSink<>(config, transports, new ResultSinkFunction());
        dataStream.addSink(sink);
    }

    private class ResultSinkFunction implements ElasticsearchSinkFunction<Row> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void process(Row event, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(event));
        }

        private IndexRequest createIndexRequest(Row row) {
            Map<String,Object> m = new HashMap<>();
            String id = null;
            for (int i = 0 ; i < fieldNames.length ; i++ ) {
                String fieldName = fieldNames[i];
                Object val = row.getField(i);
                if (fieldName.equals(idFieldName)) {
                    id = val.toString();
                } else {
                    m.put(fieldName, val);
                }
            }
            String source;
            try {
                source = objectMapper.writeValueAsString(m);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
            LOG.info("id={}, source={}", id, source);
            return Requests.indexRequest()
                    .index(index)
                    .type(type)
                    .id(id)
                    .source(source);
        }
    }
}
