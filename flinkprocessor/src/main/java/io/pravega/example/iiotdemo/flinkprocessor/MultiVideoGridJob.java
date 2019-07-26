package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.lang.Math.max;

public class MultiVideoGridJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MultiVideoGridJob.class);

    public MultiVideoGridJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = MultiVideoGridJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
//            createStream(appConfiguration.getInputStreamConfig());
            createStream(appConfiguration.getOutputStreamConfig());

            // Start at the current tail.
            StreamCut startStreamCut = getStreamInfo(appConfiguration.getInputStreamConfig().stream).getTailStreamCut();
//            StreamCut startStreamCut = StreamCut.UNBOUNDED;

            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream, startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env.addSource(flinkPravegaReader);

            DataStream<VideoFrame> inVideoFrames = inChunkedVideoFrames
                    .keyBy("camera", "ssrc", "timestamp", "frameNumber")
                    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                    .trigger(new ChunkedVideoFrameTrigger())
                    .process(new ChunkedVideoFrameReassembler());

            DataStream<VideoFrame> inVideoFramesWithTimestamps = inVideoFrames.assignTimestampsAndWatermarks(
                    new BoundedOutOfOrdernessTimestampExtractor<VideoFrame>(Time.milliseconds(100)) {
                @Override
                public long extractTimestamp(VideoFrame element) {
                    return element.timestamp.getTime();
                }
            });
            inVideoFramesWithTimestamps.printToErr();

            // Resize all input images.
            int imageWidth = 50;
            int imageHeight = 50;
            DataStream<VideoFrame> resizedVideoFrames = inVideoFramesWithTimestamps.map(frame -> {
                ImageResizer resizer = new ImageResizer(imageWidth, imageHeight);
                frame.data = resizer.resize(frame.data);
                return frame;
            });
            resizedVideoFrames.printToErr();

            // Aggregate resized images.
            // For each 100 millisecond window, we take the last image from each camera.
            // Then these images are combined in a square grid.
            // To maintain ordering in the output images, we use parallelism 1 for all subsequent operations.
            int camera = 1000;
            int ssrc = new Random().nextInt();
            DataStream<VideoFrame> outVideoFrames = resizedVideoFrames
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                    .aggregate(new ImageAggregator(imageWidth, imageHeight, camera, ssrc))
                    .setParallelism(1);
            outVideoFrames.printToErr();

            // Split video frames into chunks of 1 MB or less.
            DataStream<ChunkedVideoFrame> outChunkedVideoFrames = outVideoFrames
                    .flatMap(new VideoFrameChunker())
                    .setParallelism(1);

            // Write chunks to Pravega encoded as JSON.
            FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getOutputStreamConfig().stream)
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .build();
            outChunkedVideoFrames
                    .addSink(flinkPravegaWriter)
                    .setParallelism(1);

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class ImageAggregatorAccum {
        // Map from camera to last image data.
        public Map<Integer, byte[]> images = new HashMap<>();
        // Maximum timestamp from cameras.
        public Timestamp timestamp = new Timestamp(0);
    }

    public static class ImageAggregator implements AggregateFunction<VideoFrame, ImageAggregatorAccum, VideoFrame> {
        private final int imageWidth;
        private final int imageHeight;
        private final int camera;
        private final int ssrc;
        // frameNumber is part of the state. There is only a single partition so this can be an ordinary instance variable.
        private int frameNumber;

        public ImageAggregator(int imageWidth, int imageHeight, int camera, int ssrc) {
            this.imageWidth = imageWidth;
            this.imageHeight = imageHeight;
            this.camera = camera;
            this.ssrc = ssrc;
        }

        @Override
        public ImageAggregatorAccum createAccumulator() {
            return new ImageAggregatorAccum();
        }

        @Override
        public VideoFrame getResult(ImageAggregatorAccum accum) {
            VideoFrame videoFrame = new VideoFrame();
            videoFrame.camera = camera;
            videoFrame.ssrc = ssrc;
            videoFrame.timestamp = accum.timestamp;
            videoFrame.frameNumber = frameNumber;
            ImageGridBuilder builder = new ImageGridBuilder(imageWidth, imageHeight, accum.images.size());
            builder.addImages(accum.images);
            videoFrame.data = builder.getOutputImageBytes("png");
            frameNumber++;
            return videoFrame;
        }

        @Override
        public ImageAggregatorAccum add(VideoFrame value, ImageAggregatorAccum accum) {
            accum.images.put(value.camera, value.data);
            accum.timestamp = new Timestamp(max(accum.timestamp.getTime(), value.timestamp.getTime()));
            return accum;
        }

        @Override
        public ImageAggregatorAccum merge(ImageAggregatorAccum a, ImageAggregatorAccum b) {
            // TODO: Accumulator can be made more efficient when multiple frames from the same camera can be merged. For now, don't merge.
            return null;
        }

    }
}
