package io.pravega.example.iiotdemo.flinkprocessor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ChunkedEvent {
    public short ChunkIndex;
    public short FinalChunkIndex;
    public String EventUUID;
    public ByteBuffer Data;

    public ChunkedEvent() {
    }

    public ChunkedEvent(int chunkIndex, int finalChunkIndex, String eventUUID, String data) {
        ChunkIndex = (short) chunkIndex;
        FinalChunkIndex = (short) finalChunkIndex;
        EventUUID = eventUUID;
        Data = StandardCharsets.UTF_8.encode(data);
    }

    @Override
    public String toString() {
        return "ChunkedEvent{" +
                "ChunkIndex=" + ChunkIndex +
                ", FinalChunkIndex=" + FinalChunkIndex +
                ", EventUUID='" + EventUUID + '\'' +
                '}';
    }
}
