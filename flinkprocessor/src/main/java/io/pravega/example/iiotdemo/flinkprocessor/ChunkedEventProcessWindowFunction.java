package io.pravega.example.iiotdemo.flinkprocessor;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.ByteBuffer;
import java.util.stream.StreamSupport;

public class ChunkedEventProcessWindowFunction extends ProcessWindowFunction<ChunkedEvent, ByteBuffer, Tuple, TimeWindow> {
    @Override
    public void process(Tuple key, Context context, Iterable<ChunkedEvent> elements, Collector<ByteBuffer> out) throws Exception {
        // TODO: Ensure that all chunks are present and place in the correct order.
        int totalSize = StreamSupport.stream(elements.spliterator(), false).mapToInt((e) -> e.Data.remaining()).sum();
        ByteBuffer output = ByteBuffer.allocate(totalSize);
        for (ChunkedEvent chunk: elements) {
            output.put(chunk.Data);
        }
        output.flip();
        out.collect(output);
    }
}
