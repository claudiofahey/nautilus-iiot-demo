package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

public class LargeEventReassemblyTestJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(LargeEventReassemblyTestJob.class);

    public LargeEventReassemblyTestJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = LargeEventReassemblyTestJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            DataStream<ChunkedEvent> ds1 = env.fromElements(
                    new ChunkedEvent(0, 1, "a", "AA"),
                    new ChunkedEvent(0, 1, "b", "BB"),
                    new ChunkedEvent(1, 1, "b", "BB"),
                    new ChunkedEvent(1, 1, "a", "AA"),
                    new ChunkedEvent(0, 0, "z", "ZZ")
            );
            //ds1.printToErr();

            KeyedStream<ChunkedEvent, Tuple> ds2 = ds1.keyBy("EventUUID");
            ds2.printToErr();

            WindowedStream<ChunkedEvent, Tuple, TimeWindow> ds3 = ds2.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
            WindowedStream<ChunkedEvent, Tuple, TimeWindow> ds4 = ds3.trigger(new ChunkedEventTrigger());

            SingleOutputStreamOperator<ByteBuffer> ds5 = ds4.process(new ChunkedEventProcessWindowFunction());
            ds5.printToErr();

            SingleOutputStreamOperator<CharBuffer> ds6 = ds5.map((b) -> StandardCharsets.UTF_8.decode(b));
            ds6.printToErr();

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
