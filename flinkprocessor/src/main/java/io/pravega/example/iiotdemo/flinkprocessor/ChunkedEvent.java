package io.pravega.example.iiotdemo.flinkprocessor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ChunkedEvent {
    public short chunkIndex;
    public short finalChunkIndex;
    public String eventUUID;
    public ByteBuffer data;

    public ChunkedEvent() {
    }

    public ChunkedEvent(int chunkIndex, int finalChunkIndex, String eventUUID, String data) {
        this.chunkIndex = (short) chunkIndex;
        this.finalChunkIndex = (short) finalChunkIndex;
        this.eventUUID = eventUUID;
        this.data = StandardCharsets.UTF_8.encode(data);
    }

    @Override
    public String toString() {
        return "ChunkedEvent{" +
                "chunkIndex=" + chunkIndex +
                ", finalChunkIndex=" + finalChunkIndex +
                ", eventUUID='" + eventUUID + '\'' +
                '}';
    }
}
