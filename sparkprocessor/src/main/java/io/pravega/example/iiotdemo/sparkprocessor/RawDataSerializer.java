package io.pravega.example.iiotdemo.sparkprocessor;

public class RawDataSerializer extends JsonSerializer<RawData> {
    public RawDataSerializer() {
        super(RawData.class);
    }
}
