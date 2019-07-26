package io.pravega.example.iiotdemo.flinkprocessor;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * A class for storing a single video frame.
 */
public class VideoFrame {
    // Unique ID for this video stream.
    public int camera;
    // Random source identifier used to avoid corruption if multiple sources use the same camera frameNumber.
    // See https://tools.ietf.org/html/rfc3550.
    public int ssrc;
    public Timestamp timestamp;
    public int frameNumber;
    // PNG-encoded image.
    public byte[] data;

    public VideoFrame() {
    }

    public VideoFrame(VideoFrame frame) {
        this.camera = frame.camera;
        this.ssrc = frame.ssrc;
        this.timestamp = frame.timestamp;
        this.frameNumber = frame.frameNumber;
        this.data = frame.data;
    }

    @Override
    public String toString() {
        int sizeToPrint = data.length;
        int maxSizeToPrint = 10;
        if (sizeToPrint > maxSizeToPrint) {
            sizeToPrint = maxSizeToPrint;
        }
        byte[] dataBytes = Arrays.copyOf(data, sizeToPrint);
        String dataStr = Arrays.toString(dataBytes);
        return "VideoFrame{" +
                "camera=" + camera +
                ", ssrc=" + ssrc +
                ", timestamp=" + timestamp +
                ", frameNumber=" + frameNumber +
                ", data(" + data.length + ")=" + dataStr +
                "}";
    }
}
