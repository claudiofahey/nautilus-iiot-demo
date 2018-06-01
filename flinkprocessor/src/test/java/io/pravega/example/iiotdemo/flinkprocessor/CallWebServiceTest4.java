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
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class CallWebServiceTest4 {
    private static Logger log = LoggerFactory.getLogger(CallWebServiceTest4.class);

    @Test
    public void Test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        AssignerWithPeriodicWatermarks<InputData> timestampExtractor = new BoundedOutOfOrdernessTimestampExtractor<InputData>(Time.milliseconds(1)) {
            @Override
            public long extractTimestamp(InputData element) {
                return element.timestamp;
            }
        };

        DataStream<InputData> timestamped = ds.assignTimestampsAndWatermarks(timestampExtractor).name("Extract Event Time");
        KeyedStream<InputData, Tuple> keyedStream = timestamped.keyBy("device_id");
        keyedStream.reduce(null);
        keyedStream
            .window(SlidingEventTimeWindows.of(Time.milliseconds(1000), Time.milliseconds(1000)))
            .process(new MyProcessFunc());
//         = timestamped.keyBy("device_id");
//        keyedStream.printToErr();
//        AsyncDataStream.unorderedWait(ds, new AsyncFunc(), 10000, TimeUnit.MILLISECONDS, 100).printToErr();
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
                    "mystate", // the state name
                    TypeInformation.of(new TypeHint<String>() {})); // type information
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
//            for (InputData element: elements) {
//                log.info("process: key={}, element={}", key, element);
//            }
            CompletableFuture future = asyncHttpClient
                .preparePost(url)
                .setBody(objectMapper.writeValueAsBytes(elements))
                .execute()
                .toCompletableFuture()
                .thenApply(Response::getResponseBody)
                .thenAccept((String result) -> {
                    log.info("process: result={}", result);
                });
            future.get();
        }
    }

    private static class AsyncFunc extends RichAsyncFunction<InputData, OutputData> {
        private transient AsyncHttpClient asyncHttpClient;
        private transient ObjectMapper objectMapper;
        private transient ValueState<String> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            asyncHttpClient = asyncHttpClient();
            objectMapper = new ObjectMapper();
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>(
                            "mystate", // the state name
                            TypeInformation.of(new TypeHint<String>() {})); // type information
            // TODO: below returns java.lang.UnsupportedOperationException: State is not supported in rich async functions.
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void close() throws Exception {
            asyncHttpClient.close();
        }

        @Override
        public void asyncInvoke(final InputData input, final ResultFuture<OutputData> resultFuture) throws Exception {
//            String url = "http://httpbin.org/post";
//            String url = "http://localhost:8123/post";
            String url = "http://localhost:5001/predict";
            String stateValue = state.value();
            log.info("stateValue={}", stateValue);
            asyncHttpClient
                .preparePost(url)
                .setBody(objectMapper.writeValueAsBytes(input))
                .execute()
                .toCompletableFuture()
                .thenApply(Response::getResponseBody)
                .thenAccept((String result) -> {
                    try {
                        List<OutputData> outputData = objectMapper.readValue(
                            result,
                            objectMapper.getTypeFactory().constructCollectionType(List.class, OutputData.class));
                        state.update("new state");
                        resultFuture.complete(outputData);
                    } catch (IOException e) {
                        resultFuture.completeExceptionally(e);
                    }
                })
                .exceptionally(e -> {
                    resultFuture.completeExceptionally(e);
                    return null;
                });
        }
    }
}
