package io.pravega.example.iiotdemo.flinkprocessor;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;

public class VideoFrame {
    // Unique ID for this video stream.
    public int camera;
    // Random source identifier used to avoid corruption if multiple sources use the same camera frameNumber.
    // See https://tools.ietf.org/html/rfc3550.
    public int ssrc;
    public Timestamp timestamp;
    public int frameNumber;
    // PNG-encoded image.
    // Note that Jackson serialization does not properly handle ByteBuffer with non-zero position.
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
