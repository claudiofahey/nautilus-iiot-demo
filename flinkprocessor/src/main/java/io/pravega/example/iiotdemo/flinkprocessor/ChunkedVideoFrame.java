package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChunkedVideoFrame extends VideoFrame {
//    public int camera;
//    public int ssrc;
//    public Timestamp timestamp;
//    public int frameNumber;
//    public String data;

    public short chunkIndex;
    public short finalChunkIndex;

    public ChunkedVideoFrame() {
    }

    @Override
    public String toString() {
        return "ChunkedVideoFrame{" +
//                "routingKey='" + routingKey + '\'' +
                "camera=" + camera +
                ", ssrc=" + ssrc +
                ", timestamp=" + timestamp +
                ", frameNumber=" + frameNumber +
                ", data='" + data + '\'' +
                ", chunkIndex=" + chunkIndex +
                ", finalChunkIndex=" + finalChunkIndex +
                '}';
    }
}
