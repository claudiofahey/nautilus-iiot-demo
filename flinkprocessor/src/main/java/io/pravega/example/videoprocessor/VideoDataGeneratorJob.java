package io.pravega.example.videoprocessor;

import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.example.iiotdemo.flinkprocessor.AbstractJob;
import io.pravega.example.iiotdemo.flinkprocessor.AppConfiguration;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;

/**
 * This job simulates writing video from multiple cameras to Pravega.
 */
public class VideoDataGeneratorJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(VideoDataGeneratorJob.class);

    public VideoDataGeneratorJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = VideoDataGeneratorJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(appConfiguration.getOutputStreamConfig());

            // Generate a stream of sequential frame numbers along with timestamps.
            double framesPerSec = 1.0;
            DataStream<Tuple2<Integer,Long>> frameNumbers = env.fromCollection(
                    new FrameNumberIterator(framesPerSec),
                    TypeInformation.of(new TypeHint<Tuple2<Integer,Long>>(){}));

            // Generate a stream of video frames.
            int[] cameras = new int[]{0, 1, 2, 3};
            int ssrc = new Random().nextInt();
            int width = 1000;
            int height = width;
            DataStream<VideoFrame> videoFrames =
                    frameNumbers.flatMap(new FlatMapFunction<Tuple2<Integer,Long>, VideoFrame>() {
                        @Override
                        public void flatMap(Tuple2<Integer,Long> in, Collector<VideoFrame> out) {
                            for (int camera: cameras) {
                                VideoFrame frame = new VideoFrame();
                                frame.camera = camera;
                                frame.ssrc = ssrc + camera;
                                frame.timestamp = new Timestamp(in.f1);
                                frame.frameNumber = in.f0;
                                frame.data = new ImageGenerator(width, height).generate(frame.camera, frame.frameNumber);
                                frame.hash = frame.calculateHash();
                                out.collect(frame);
                            }
                        }
                    });
            videoFrames.printToErr();

            // Split video frames into chunks of 1 MB or less. We must account for base-64 encoding, header fields, and JSON. Use 0.5 MB to be safe.
            int chunkSizeBytes = 512*1024;
            DataStream<ChunkedVideoFrame> chunkedVideoFrames = videoFrames.flatMap(new VideoFrameChunker(chunkSizeBytes));
            chunkedVideoFrames.printToErr();

            // Write chunks to Pravega encoded as JSON.
            FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getOutputStreamConfig().stream)
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .build();
            chunkedVideoFrames.addSink(flinkPravegaWriter);

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
