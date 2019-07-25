package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChunkedVideoFrame extends VideoFrame {
    public short chunkIndex;
    public short finalChunkIndex;

    public ChunkedVideoFrame() {
    }

    public ChunkedVideoFrame(VideoFrame frame) {
        super(frame);
    }

    @Override
    public String toString() {
        return "ChunkedVideoFrame{" +
                super.toString() +
                ", chunkIndex=" + chunkIndex +
                ", finalChunkIndex=" + finalChunkIndex +
                '}';
    }
}
