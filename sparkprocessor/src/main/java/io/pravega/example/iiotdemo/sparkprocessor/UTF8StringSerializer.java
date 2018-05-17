package io.pravega.example.iiotdemo.sparkprocessor;

import io.pravega.client.stream.Serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UTF8StringSerializer implements Serializer<String>, Serializable {
    @Override
    public ByteBuffer serialize(String value) {
            return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return new String(serializedValue.array(), StandardCharsets.UTF_8);
    }
}
