package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaWriter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Random;

import static java.lang.Math.min;

public class VideoWriterTestJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(VideoWriterTestJob.class);

    public VideoWriterTestJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = VideoWriterTestJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(appConfiguration.getOutputStreamConfig());

            // Generate a stream of sequential frame numbers along with timestamps.
            double framesPerSec = 1.0;
            DataStream<Tuple2<Integer,Long>> frameNumbers = env.fromCollection(
                    new FrameNumberIterator(framesPerSec),
                    TypeInformation.of(new TypeHint<Tuple2<Integer,Long>>(){}));

            // Generate a stream of video frames.
            int[] cameras = new int[]{0, 1};
            int ssrc = new Random().nextInt();
            int width = 100;
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
                                frame.data = ByteBuffer.wrap(new ImageGenerator(width, height).generate(frame.camera, frame.frameNumber));
                                out.collect(frame);
                            }
                        }
                    });
            videoFrames.printToErr();

            // Split video frames into chunks of 1 MB or less. We must account for base-64 encoding, header fields, and JSON. Use 0.5 MB to be safe.
            int chunkSizeBytes = 512*1024;
            DataStream<ChunkedVideoFrame> chunkedVideoFrames =
                    videoFrames.flatMap(new FlatMapFunction<VideoFrame, ChunkedVideoFrame>() {
                        @Override
                        public void flatMap(VideoFrame in, Collector<ChunkedVideoFrame> out) {
                            int numChunks = (in.data.remaining() - 1) / chunkSizeBytes + 1;
                            for (int chunkIndex = 0 ; chunkIndex < numChunks ; chunkIndex++) {
                                ChunkedVideoFrame frame = new ChunkedVideoFrame(in);
                                frame.data.position(in.data.position() + chunkIndex * chunkSizeBytes);
                                frame.data.limit(in.data.position() + min((chunkIndex + 1) * chunkSizeBytes, in.data.remaining()));

                                // Jackson serialization does not properly handle ByteBuffer with non-zero position so we need to create a new ByteBuffer.
                                byte[] chunkData = new byte[frame.data.remaining()];
                                frame.data.get(chunkData);
                                frame.data = ByteBuffer.wrap(chunkData);

                                frame.chunkIndex = (short) chunkIndex;
                                frame.finalChunkIndex = (short) (numChunks - 1);
                            out.collect(frame);
                            }
                        }
                    });
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
