package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaJsonTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MultiVideoGridJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MultiVideoGridJob.class);

    public MultiVideoGridJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = MultiVideoGridJob.class.getName();
            // TODO: Job does not work with parallelism > 1
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getInputStreamConfig());
            StreamCut tailStreamCut = getStreamInfo(appConfiguration.getInputStreamConfig().stream).getTailStreamCut();
//            StreamCut tailStreamCut = StreamCut.UNBOUNDED;
            TableSchema inputSchema = TableSchema.builder()
                    .field("timestamp", Types.SQL_TIMESTAMP())
                    .field("frame_number", Types.INT())
                    .field("camera", Types.INT())
                    .field("ssrc", Types.INT())
                    .field("data", Types.PRIMITIVE_ARRAY(Types.BYTE()))
                    .build();
            FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                    .forStream(appConfiguration.getInputStreamConfig().stream, tailStreamCut, StreamCut.UNBOUNDED)
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .failOnMissingField(true)
                    .withRowtimeAttribute("timestamp", new ExistingField("timestamp"), new BoundedOutOfOrderTimestamps(100L))
                    .withSchema(inputSchema)
                    .build();
            tableEnv.registerTableSource("video", source);
            Table t1 = tableEnv.scan("video");
            t1.printSchema();

            tableEnv.registerFunction("ImageResizer", new ImageResizer());
            tableEnv.registerFunction("ImageAggregator", new ImageAggregator());

            Table t2 = t1.select("timestamp, camera, ImageResizer(data) as data");
            t2.printSchema();

            Table t3 = t2
                    .window(Tumble.over("100.millis").on("timestamp").as("w"))
                    .groupBy("w")
                    .select("w.start, w.end, ImageAggregator(camera, data) as data");
            t3.printSchema();

            tableEnv.toAppendStream(t3, Row.class).printToErr();

            // TODO: write output to new Pravega stream

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class ImageResizer extends ScalarFunction {
        public byte[] eval(byte[] image) {
            // TODO: resize image
            return image;
        }
    }

    public static class ImageAggregatorAccum {
        // Map from camera to last image data.
        public Map<Integer, byte[]> images = new HashMap<>();
    }

    public static class ImageAggregator extends AggregateFunction<String, ImageAggregatorAccum> {
        @Override
        public ImageAggregatorAccum createAccumulator() {
            return new ImageAggregatorAccum();
        }

        @Override
        public String getValue(ImageAggregatorAccum accum) {
            // TODO: combine images
            return StreamSupport.stream(accum.images.keySet().spliterator(), false)
                    .map(Object::toString)
                    .collect(Collectors.joining("+"));
        }

        public void accumulate(ImageAggregatorAccum accum, int camera, byte[] data) {
            accum.images.put(camera, data);
        }
    }
}
