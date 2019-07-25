package io.pravega.example.iiotdemo.flinkprocessor;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.ByteBuffer;
import java.util.stream.StreamSupport;

public class ChunkedVideoFrameProcessWindowFunction extends ProcessWindowFunction<ChunkedVideoFrame, VideoFrame, Tuple, TimeWindow> {
    @Override
    public void process(Tuple key, Context context, Iterable<ChunkedVideoFrame> elements, Collector<VideoFrame> out) throws Exception {
        // TODO: Ensure that all chunks are present and place in the correct order.
        int totalSize = StreamSupport.stream(elements.spliterator(), false).mapToInt((e) -> e.data.remaining()).sum();
        ChunkedVideoFrame firstChunk = elements.iterator().next();
        VideoFrame videoFrame = new VideoFrame();
        videoFrame.camera = firstChunk.camera;
        videoFrame.ssrc = firstChunk.ssrc;
        videoFrame.timestamp = firstChunk.timestamp;
        videoFrame.frameNumber = firstChunk.frameNumber;
        videoFrame.data = ByteBuffer.allocate(totalSize);
        for (ChunkedVideoFrame chunk: elements) {
            videoFrame.data.put(chunk.data);
        }
        videoFrame.data.flip();
        out.collect(videoFrame);
    }
}
