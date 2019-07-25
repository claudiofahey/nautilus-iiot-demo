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

    public VideoFrame(VideoFrame frame) {
        this.camera = frame.camera;
        this.ssrc = frame.ssrc;
        this.timestamp = frame.timestamp;
        this.frameNumber = frame.frameNumber;
        this.data = frame.data.duplicate();
    }

    @Override
    public String toString() {
        int sizeToPrint = data.remaining();
        int maxSizeToPrint = 10;
        if (sizeToPrint > maxSizeToPrint) {
            sizeToPrint = maxSizeToPrint;
        }
        byte[] dataBytes = new byte[sizeToPrint];
        int orgPosition = data.position();
        data.get(dataBytes);
        data.position(orgPosition);
        String dataStr = Arrays.toString(dataBytes);
        return "VideoFrame{" +
                "camera=" + camera +
                ", ssrc=" + ssrc +
                ", timestamp=" + timestamp +
                ", frameNumber=" + frameNumber +
                ", data(" + data.remaining() + ")=" + dataStr +
                "}";
    }
}
