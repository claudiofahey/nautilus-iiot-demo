package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.ByteBuffer;

import static java.lang.Math.min;

/**
 * A FlatMapFunction to create ChunkedVideoFrame instances from a VideoFrame.
 * The chunk size must account for base-64 encoding, header fields, and JSON.
 */
class VideoFrameChunker implements FlatMapFunction<VideoFrame, ChunkedVideoFrame> {
    private final int chunkSizeBytes;

    public VideoFrameChunker() {
        this.chunkSizeBytes = 512*1024;
    }

    public VideoFrameChunker(int chunkSizeBytes) {
        this.chunkSizeBytes = chunkSizeBytes;
    }

    @Override
    public void flatMap(VideoFrame in, Collector<ChunkedVideoFrame> out) {
        int numChunks = (in.data.remaining() - 1) / chunkSizeBytes + 1;
        for (int chunkIndex = 0 ; chunkIndex < numChunks ; chunkIndex++) {
            ChunkedVideoFrame frame = new ChunkedVideoFrame(in);
            frame.data.position(in.data.position() + chunkIndex * chunkSizeBytes);
            frame.data.limit(in.data.position() + min((chunkIndex + 1) * chunkSizeBytes, in.data.remaining()));

            // Jackson serialization does not properly handle ByteBuffer with non-zero position so we need to create a new ByteBuffer.
            byte[] chunkData = new byte[frame.data.remaining()];
            frame.data.get(chunkData);
            frame.data = ByteBuffer.wrap(chunkData);

            frame.chunkIndex = (short) chunkIndex;
            frame.finalChunkIndex = (short) (numChunks - 1);
        out.collect(frame);
        }
    }
}
