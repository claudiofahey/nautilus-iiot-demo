package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MultiVideoGridJob2 extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MultiVideoGridJob2.class);

    public MultiVideoGridJob2(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = MultiVideoGridJob2.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(appConfiguration.getInputStreamConfig());
            createStream(appConfiguration.getOutputStreamConfig());

            // Start at the current tail.
//            StreamCut startStreamCut = getStreamInfo(appConfiguration.getInputStreamConfig().stream).getTailStreamCut();
            StreamCut startStreamCut = StreamCut.UNBOUNDED;

            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream, startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            DataStream<ChunkedVideoFrame> chunkedVideoFrames = env.addSource(flinkPravegaReader);

            DataStream<VideoFrame> videoFrames = chunkedVideoFrames.map(frame -> (VideoFrame) frame);

            DataStream<VideoFrame> videoFramesWithTimestamps = videoFrames.assignTimestampsAndWatermarks(
                    new BoundedOutOfOrdernessTimestampExtractor<VideoFrame>(Time.milliseconds(100)) {
                @Override
                public long extractTimestamp(VideoFrame element) {
                    return element.timestamp.getTime();
                }
            });

            // Resize all input images.
            int imageWidth = 50;
            int imageHeight = 50;
            DataStream<VideoFrame> resizedVideoFrames = videoFramesWithTimestamps.map(frame -> {
                ImageResizer resizer = new ImageResizer(imageWidth, imageHeight);
                frame.data = ByteBuffer.wrap(resizer.resize(frame.data.array()));
                return frame;
            });
            resizedVideoFrames.printToErr();

            // Aggregate resized images.
            // For each 100 millisecond window, we take the last image from each camera.
            // Then these images are combined in a square grid.
            // TODO: The window is never triggered if parallelism > 1. See https://emcnautilus.slack.com/archives/C0LJMGMNH/p1559586022017100.
//            tableEnv.registerFunction("ImageAggregator", new ImageAggregator(imageWidth, imageHeight));
//            Table t3 = t2
//                    .window(Tumble.over("100.millis").on("timestamp").as("w"))
//                    .groupBy("w")
//                    .select("0 as camera, 0 as ssrc, 0 as frameNumber, w.end as timestamp, '' as routing_key" +
//                            ", ImageAggregator(camera, data) as data");
//            t3.printSchema();
//            tableEnv.toAppendStream(t3.select("timestamp"), Row.class).printToErr();
            DataStream<VideoFrame> outVideoFrames = resizedVideoFrames
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                    .aggregate(new ImageAggregator(imageWidth, imageHeight));
            outVideoFrames.printToErr();

            DataStream<ChunkedVideoFrame> outChunkedVideoFrames = outVideoFrames.map(ChunkedVideoFrame::new);

            // Write chunks to Pravega encoded as JSON.
            FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getOutputStreamConfig().stream)
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .build();
            outChunkedVideoFrames.addSink(flinkPravegaWriter);

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class ImageAggregatorAccum {
        // Map from camera to last image data.
        public Map<Integer, byte[]> images = new HashMap<>();
        public VideoFrame videoFrame = new VideoFrame();
    }

    public static class ImageAggregator implements AggregateFunction<VideoFrame, ImageAggregatorAccum, VideoFrame> {

        private final int imageWidth;
        private final int imageHeight;

        public ImageAggregator(int imageWidth, int imageHeight) {
            this.imageWidth = imageWidth;
            this.imageHeight = imageHeight;
        }

        @Override
        public ImageAggregatorAccum createAccumulator() {
            return new ImageAggregatorAccum();
        }

        @Override
        public VideoFrame getResult(ImageAggregatorAccum accum) {
            ImageGridBuilder builder = new ImageGridBuilder(imageWidth, imageHeight, accum.images.size());
            builder.addImages(accum.images);
            VideoFrame videoFrame = accum.videoFrame;
            videoFrame.data = ByteBuffer.wrap(builder.getOutputImageBytes("png"));
            return videoFrame;
        }

        @Override
        public ImageAggregatorAccum add(VideoFrame value, ImageAggregatorAccum accum) {
            accum.images.put(value.camera, value.data.array());
            return accum;
        }

        @Override
        public ImageAggregatorAccum merge(ImageAggregatorAccum a, ImageAggregatorAccum b) {
            // TODO
            return null;
        }

    }
}
