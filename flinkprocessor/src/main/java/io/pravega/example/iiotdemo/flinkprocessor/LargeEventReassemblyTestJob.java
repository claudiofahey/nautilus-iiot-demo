package io.pravega.example.iiotdemo.flinkprocessor;

import io.pravega.connectors.flink.serialization.JsonRowDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
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
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            DataStream<ChunkedEvent> ds1 = env.fromElements(
                    new ChunkedEvent(0, 1, "a", "{\"frame_number\":0"),
                    new ChunkedEvent(0, 1, "b", "{\"frame_number\":1"),
                    new ChunkedEvent(1, 1, "b", "}"),
                    new ChunkedEvent(1, 1, "a", "}"),
                    new ChunkedEvent(0, 0, "z", "{\"frame_number\":99}")
            );
            //ds1.printToErr();

            KeyedStream<ChunkedEvent, Tuple> ds2 = ds1.keyBy("EventUUID");
            ds2.printToErr();

            WindowedStream<ChunkedEvent, Tuple, TimeWindow> ds3 = ds2.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
            WindowedStream<ChunkedEvent, Tuple, TimeWindow> ds4 = ds3.trigger(new ChunkedEventTrigger());

            SingleOutputStreamOperator<ByteBuffer> ds5 = ds4.process(new ChunkedEventProcessWindowFunction());
            ds5.printToErr();

//            SingleOutputStreamOperator<CharBuffer> ds6 = ds5.map((b) -> StandardCharsets.UTF_8.decode(b));
//            ds6.printToErr();

            TableSchema inputSchema = TableSchema.builder()
//                    .field("timestamp", Types.SQL_TIMESTAMP())
                    .field("frame_number", Types.INT())
//                    .field("camera", Types.INT())
//                    .field("ssrc", Types.INT())
//                    .field("data", Types.PRIMITIVE_ARRAY(Types.BYTE()))     // PNG file bytes
                    .build();
            RowTypeInfo rowTypeInfo = new RowTypeInfo(inputSchema.getTypes(), inputSchema.getColumnNames());
            DeserializationSchema<Row> deserializer = new JsonRowDeserializationSchema(rowTypeInfo);

            SingleOutputStreamOperator<Row> ds6 = ds5.map((r) -> deserializer.deserialize(r.array()));
            ds6.printToErr();

//            Table t1 = tableEnv.fromDataStream(ds6);
//            t1.printSchema();
//            tableEnv.toAppendStream(t1, Row.class).printToErr();

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
