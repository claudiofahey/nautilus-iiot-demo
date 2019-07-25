package io.pravega.example.iiotdemo.flinkprocessor;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;

public class VideoFrame {
    public int camera;
    public int ssrc;
    public Timestamp timestamp;
    public int frameNumber;
    public ByteBuffer data;

    public VideoFrame() {
    }

    @Override
    public String toString() {
        return "VideoFrame{" +
                "camera=" + camera +
                ", ssrc=" + ssrc +
                ", timestamp=" + timestamp +
                ", frameNumber=" + frameNumber +
                ", data='" + Arrays.toString(data.array()) + '\'' +
                '}';
    }
}
