package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
            createStream(appConfiguration.getInputStreamConfig());

            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            DataStream<ChunkedVideoFrame> chunkedVideoFrames = env.addSource(flinkPravegaReader);
            chunkedVideoFrames.printToErr();

            DataStream<VideoFrame> videoFrames = chunkedVideoFrames
                    .keyBy("camera", "ssrc", "timestamp", "frameNumber")
                    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                    .trigger(new ChunkedVideoFrameTrigger())
                    .process(new ChunkedVideoFrameProcessWindowFunction());
            videoFrames.printToErr();

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
