package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.util.Collections;

/**
 * This job reads a video stream from Pravega and writes frame metadata to the console.
 */
public class VideoReaderJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(VideoReaderJob.class);

    public VideoReaderJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = VideoReaderJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(appConfiguration.getInputStreamConfig());

            // Start at the current tail.
            StreamCut startStreamCut = getStreamInfo(appConfiguration.getInputStreamConfig().stream).getTailStreamCut();
//            StreamCut startStreamCut = StreamCut.UNBOUNDED;

            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream, startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            DataStream<ChunkedVideoFrame> chunkedVideoFrames = env.addSource(flinkPravegaReader);
            chunkedVideoFrames.printToErr();

            DataStream<VideoFrame> videoFrames = chunkedVideoFrames
                    .keyBy("camera", "ssrc", "timestamp", "frameNumber")
                    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                    .trigger(new ChunkedVideoFrameTrigger())
                    .process(new ChunkedVideoFrameReassembler());
//            videoFrames.printToErr();

            // Parse image file and obtain metadata.
            DataStream<String> frameInfo = videoFrames.map(frame -> {
                int numBytes = frame.data.remaining();
                InputStream inStream = new ByteBufferInputStream(Collections.singletonList(frame.data));
                BufferedImage inImage = ImageIO.read(inStream);
                return String.format("%s, %dx%dx%d, %d bytes",
                        inImage.toString(),
                        inImage.getWidth(),
                        inImage.getHeight(),
                        inImage.getColorModel().getNumColorComponents(),
                        numBytes);
            });
            frameInfo.printToErr();

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
