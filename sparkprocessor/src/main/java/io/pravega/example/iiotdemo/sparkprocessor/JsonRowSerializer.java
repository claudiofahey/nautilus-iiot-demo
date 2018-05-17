package io.pravega.example.iiotdemo.sparkprocessor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class JsonRowSerializer implements Serializer<Row>, Serializable {
    private static Logger log = LoggerFactory.getLogger(JsonRowSerializer.class);
    private final StructType schema;
    private final ObjectMapper objectMapper;

    public JsonRowSerializer(Configuration conf) throws Exception {
        String schemaText = conf.get("schema");
        log.info("schemaText={}", schemaText);
        this.schema = (StructType) StructType.fromJson(schemaText);
        log.info("schema={}", this.schema);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public ByteBuffer serialize(Row row) {
//        try {
            byte[] result = {};//objectMapper.writeValueAsBytes(value);
            return ByteBuffer.wrap(result);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    @Override
    public Row deserialize(ByteBuffer serializedValue) {
        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        try {
            JsonNode root = objectMapper.readTree(bin);
            Object[] values = new Object[schema.fields().length];
            for (int i = 0 ; i < schema.fields().length ; i++) {
                JsonNode node = root.get(schema.fields()[i].name());
                values[i] = node.toString(); // TODO: get rid of toString
            }
            return RowFactory.create(values);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
