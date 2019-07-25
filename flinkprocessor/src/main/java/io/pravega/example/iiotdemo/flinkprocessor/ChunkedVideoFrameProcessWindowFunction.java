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
//        int totalSize = StreamSupport.stream(elements.spliterator(), false).mapToInt((e) -> e.data.remaining()).sum();
//        ByteBuffer output = ByteBuffer.allocate(totalSize);
        VideoFrame videoFrame = new VideoFrame();
        for (ChunkedVideoFrame chunk: elements) {
            videoFrame.frameNumber = chunk.frameNumber;
            out.collect(videoFrame);
            break;
        }
//        output.flip();
//        out.collect(output);
    }
}