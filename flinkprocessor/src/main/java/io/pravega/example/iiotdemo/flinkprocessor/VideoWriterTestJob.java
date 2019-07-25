package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.Pravega;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

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
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getOutputStreamConfig());

            DataStream<Integer> ds1 = env.fromElements(0, 1, 2, 3);

            DataStream<VideoFrame> ds2 =
                    ds1.flatMap(new FlatMapFunction<Integer, VideoFrame>() {
                        @Override
                        public void flatMap(Integer frameNumber, Collector<VideoFrame> out) {
                            VideoFrame frame = new VideoFrame();
                            frame.camera = 0;
                            frame.ssrc = 0;
                            frame.timestamp = new Timestamp(System.currentTimeMillis());
                            frame.frameNumber = frameNumber;
                            frame.data = ByteBuffer.wrap(new byte[]{0, 1, 2, 3});
                            out.collect(frame);
                        }
                    });

            ds2.printToErr();

            int chunkSizeBytes = 3;

            DataStream<ChunkedVideoFrame> ds3 =
                    ds2.flatMap(new FlatMapFunction<VideoFrame, ChunkedVideoFrame>() {
                        @Override
                        public void flatMap(VideoFrame in, Collector<ChunkedVideoFrame> out) {
                            int numChunks = (in.data.remaining() - 1) / chunkSizeBytes + 1;
                            for (int chunkIndex = 0 ; chunkIndex < numChunks ; chunkIndex++) {
                                ChunkedVideoFrame frame = new ChunkedVideoFrame(in);
                                frame.data.position(in.data.position() + chunkIndex * chunkSizeBytes);
                                frame.data.limit(in.data.position() + min((chunkIndex + 1) * chunkSizeBytes, in.data.remaining()));
                                frame.chunkIndex = (short) chunkIndex;
                                frame.finalChunkIndex = (short) (numChunks - 1);
                            out.collect(frame);
                            }
                        }
                    });
            ds3.printToErr();

            FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getOutputStreamConfig().stream)
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d-%d", frame.camera, frame.ssrc))
                    .build();
            ds3.addSink(flinkPravegaWriter);

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
