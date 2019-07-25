package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaReader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VideoReaderTestJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(VideoReaderTestJob.class);

    public VideoReaderTestJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = VideoReaderTestJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getInputStreamConfig());

            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            DataStream<ChunkedVideoFrame> ds1 = env.addSource(flinkPravegaReader);
//            ds1.printToErr();

            KeyedStream<ChunkedVideoFrame, Tuple> ds2 = ds1.keyBy("camera", "ssrc", "timestamp", "frameNumber");
            ds2.printToErr();

            WindowedStream<ChunkedVideoFrame, Tuple, TimeWindow> ds3 = ds2.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
            WindowedStream<ChunkedVideoFrame, Tuple, TimeWindow> ds4 = ds3.trigger(new ChunkedVideoFrameTrigger());
            DataStream<VideoFrame> ds5 = ds4.process(new ChunkedVideoFrameProcessWindowFunction());
            ds5.printToErr();

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
