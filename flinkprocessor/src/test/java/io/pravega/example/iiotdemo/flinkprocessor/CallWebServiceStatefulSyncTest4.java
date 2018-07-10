package io.pravega.example.iiotdemo.flinkprocessor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.asynchttpclient.Dsl.asyncHttpClient;

// This demonstrates how to call a stateful web service from a Flink job.
// The web service could be a Python Flask application, TF Serving, an R application, etc..
// Because AsyncIO does not currently allow state, we must use a sync call to a ProcessWindowFunction.
@Ignore()
public class CallWebServiceStatefulSyncTest4 {
    private static Logger log = LoggerFactory.getLogger(CallWebServiceStatefulSyncTest4.class);

    @Test
    public void Test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create data simulating multiple devices sending sensor data periodically.
        List<InputData> list = new ArrayList<>();
        for (int t = 0 ; t < 9 ; t++) {
            for (int d = 0 ; d < 2 ; d++) {
                InputData inputData = new InputData();
                inputData.timestamp = t*250;
                inputData.device_id = String.format("%04d", d);
                list.add(inputData);
            }
        }
        DataStream<InputData> ds = env.fromCollection(list);
        ds.printToErr();

        // Assign event time.
        AssignerWithPeriodicWatermarks<InputData> timestampExtractor = new BoundedOutOfOrdernessTimestampExtractor<InputData>(Time.milliseconds(1)) {
            @Override
            public long extractTimestamp(InputData element) {
                return element.timestamp;
            }
        };

        DataStream<InputData> timestamped = ds.assignTimestampsAndWatermarks(timestampExtractor).name("Extract Event Time");
        KeyedStream<InputData, Tuple> keyedStream = timestamped.keyBy("device_id");
        keyedStream
                // build a 1 second window for each device.
                .window(SlidingEventTimeWindows.of(Time.milliseconds(1000), Time.milliseconds(1000)))
                .process(new MyProcessFunc())
                .printToErr();
//         = timestamped.keyBy("device_id");
        env.execute();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InputData implements Serializable {
        public long timestamp;
        public String device_id;
        public double temp_celsius;
        public double vibration1;
        public double vibration2;

        @Override
        public String toString() {
            return "InputData{" +
                    "timestamp=" + timestamp +
                    ", device_id='" + device_id + '\'' +
                    ", temp_celsius=" + temp_celsius +
                    ", vibration1=" + vibration1 +
                    ", vibration2=" + vibration2 +
                    '}';
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OutputData implements Serializable {
        public long timestamp;
        public long new_timestamp;
        public String device_id;
        public double temp_celsius;
        public double vibration1;
        public double vibration2;

        @Override
        public String toString() {
            return "OutputData{" +
                    "timestamp=" + timestamp +
                    ", new_timestamp=" + new_timestamp +
                    ", device_id='" + device_id + '\'' +
                    ", temp_celsius=" + temp_celsius +
                    ", vibration1=" + vibration1 +
                    ", vibration2=" + vibration2 +
                    '}';
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WebSvcInput extends InputData {
        public String state;
        public List<InputData> data;

        @Override
        public String toString() {
            return "WebSvcInput{" +
                    "state='" + state + '\'' +
                    ", data=" + data +
                    '}';
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WebSvcOutput extends OutputData {
        public String state;
        public List<OutputData> data;

        @Override
        public String toString() {
            return "WebSvcOutput{" +
                    "state='" + state + '\'' +
                    ", data=" + data +
                    '}';
        }
    }

    private static class MyProcessFunc extends ProcessWindowFunction<InputData, OutputData, Tuple, TimeWindow> {
        private transient AsyncHttpClient asyncHttpClient;
        private transient ObjectMapper objectMapper;
        private transient ValueState<String> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            asyncHttpClient = asyncHttpClient();
            objectMapper = new ObjectMapper();
            ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>(
                    "mystate",
                    TypeInformation.of(new TypeHint<String>() {}));
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void close() throws Exception {
            asyncHttpClient.close();
        }

        @Override
        public void process(Tuple key, Context context, Iterable<InputData> elements, Collector<OutputData> out) throws Exception {
            String url = "http://localhost:8123/post";
//            String url = "http://localhost:5001/predict";
            String stateValue = state.value();
            log.info("process: stateValue={}", stateValue);
            WebSvcInput webSvcInput = new WebSvcInput();
            webSvcInput.state = stateValue;
            webSvcInput.data = new ArrayList<>();
            elements.forEach(webSvcInput.data::add);
            CompletableFuture future = asyncHttpClient
                .preparePost(url)
                .setBody(objectMapper.writeValueAsBytes(webSvcInput))
                .execute()
                .toCompletableFuture()
                .thenApply(Response::getResponseBody)
                .thenAccept((String result) -> {
                    log.info("process: result={}", result);
                    // TODO: parse result and build OutputData.
                    OutputData outputData = new OutputData();
                    outputData.device_id = "test";
                    out.collect(outputData);
                    try {
                        state.update(result);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            future.get();
        }
    }

}
