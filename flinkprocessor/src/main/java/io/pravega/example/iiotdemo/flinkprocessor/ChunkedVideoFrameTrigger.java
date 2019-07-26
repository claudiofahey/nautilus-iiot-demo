package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A Trigger that immediately fires when a final chunk is received.
 */
public class ChunkedVideoFrameTrigger extends Trigger<ChunkedVideoFrame, Window> {
    @Override
    public TriggerResult onElement(ChunkedVideoFrame element, long timestamp, Window window, TriggerContext ctx) throws Exception {
        // TODO: This assumes final chunk is last. Is ordering of chunks guaranteed?
        if (element.chunkIndex == element.finalChunkIndex)
            return TriggerResult.FIRE_AND_PURGE;
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(Window window, TriggerContext ctx) throws Exception {
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(Window window, OnMergeContext ctx) throws Exception {
    }
}
