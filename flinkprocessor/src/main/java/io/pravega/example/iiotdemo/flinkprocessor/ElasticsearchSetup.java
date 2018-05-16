package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.ResourceAlreadyExistsException;
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
import java.net.URL;
import java.util.Collections;
import java.util.Map;

public class ElasticsearchSetup implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSetup.class);

    private String host;
    private int port;
    private String cluster;
    private String index;
    private String type;
    private boolean isDeleteIndex;

    public ElasticsearchSetup(String host, int port, String cluster, String index, String type, boolean isDeleteIndex) {
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
}
