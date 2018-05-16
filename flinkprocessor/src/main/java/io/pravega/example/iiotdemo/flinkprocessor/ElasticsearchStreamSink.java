package io.pravega.example.iiotdemo.flinkprocessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;

public class ElasticsearchStreamSink implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchStreamSink.class);

    private Table table;
    private String idFieldName;
    private String host;
    private int port;
    private String cluster;
    private String index;
    private String type;
    private boolean isDeleteIndex;

    public ElasticsearchStreamSink(Table table, String idFieldName, String host, int port, String cluster, String index, String type, boolean isDeleteIndex) {
        this.table = table;
        this.idFieldName = idFieldName;
        this.host = host;
        this.port = port;
        this.cluster = cluster;
        this.index = index;
        this.type = type;
        this.isDeleteIndex = isDeleteIndex;
    }

    public void setup() throws Exception {
        Settings settings = Settings.builder()
            .put("cluster.name", cluster)
            .put("client.transport.sniff", "false")
            .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));

        if (isDeleteIndex) {
            LOG.info("Deleting old Elasticsearch index");
            try {
                client.admin().indices().delete(Requests.deleteIndexRequest(index)).actionGet();
            } catch (IndexNotFoundException e) {
                // Ignore exception.
            }
        }

        LOG.info("Creating Elasticsearch Index");
        String fileName = String.format("%s-elastic-index.json", index);
        String indexBody = getTemplate(fileName, Collections.singletonMap("type", type));
        try {
            client.admin().indices().create(Requests.createIndexRequest(index).source(indexBody)).actionGet();
        } catch (ResourceAlreadyExistsException e) {
            // Ignore exception.
        }
    }

    private String getTemplate(String file, Map<String, String> values) throws Exception {
        URL url = getClass().getClassLoader().getResource(file);
        if (url == null) {
            throw new IllegalStateException("Template file " + file + " not found");
        }
        String body = IOUtils.toString(url.openStream(), "UTF-8");
        for (Map.Entry<String, String> value : values.entrySet()) {
            body = body.replace("@@" + value.getKey() + "@@", value.getValue());
        }
        return body;
    }

    public void addSink() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", cluster);
        config.put("client.transport.sniff", "false");
        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName(host), port));
//        DataStream<Row> ds = table.tableEnv()..toAppendStream(table, Row.class);
//        return new ElasticsearchSink<>(config, transports, new ResultSinkFunction());
    }

    private class ResultSinkFunction implements ElasticsearchSinkFunction<Row> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void process(Row event, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(event));
        }

        private IndexRequest createIndexRequest(Row event) {
            String source = null;
            try {
                source = objectMapper.writeValueAsString(event);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
//            String id = event.getField(idFieldPos).toString();
            LOG.info("source={}", source);
            return Requests.indexRequest()
                .index(index)
                .type(type)
//                .id(id)
                .source(source);
        }
    }

}
