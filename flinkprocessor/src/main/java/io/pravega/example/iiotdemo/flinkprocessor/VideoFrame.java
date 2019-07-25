package io.pravega.example.iiotdemo.flinkprocessor;

import java.sql.Timestamp;

public class VideoFrame {
    public int camera;
    public int ssrc;
    public Timestamp timestamp;
    public int frameNumber;
    public String data;

    public VideoFrame() {
    }

    @Override
    public String toString() {
        return "VideoFrame{" +
                "camera=" + camera +
                ", ssrc=" + ssrc +
                ", timestamp=" + timestamp +
                ", frameNumber=" + frameNumber +
                ", data='" + data + '\'' +
                '}';
    }
}
