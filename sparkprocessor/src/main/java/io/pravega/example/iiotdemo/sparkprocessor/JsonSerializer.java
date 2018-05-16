package io.pravega.example.iiotdemo.sparkprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class JsonSerializer<T> implements Serializer<T>, Serializable {
    private final Class<T> valueType;
    private final ObjectMapper objectMapper;

    public JsonSerializer(Class<T> valueType) {
        this.valueType = valueType;
        this.objectMapper = new ObjectMapper();
    }

    public JsonSerializer(Class<T> valueType, ObjectMapper objectMapper) {
        this.valueType = valueType;
        this.objectMapper = objectMapper;
    }

    @Override
    public ByteBuffer serialize(T value) {
        try {
            byte[] result = objectMapper.writeValueAsBytes(value);
            return ByteBuffer.wrap(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(ByteBuffer serializedValue) {
        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        try {
            return objectMapper.readValue(bin, valueType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
