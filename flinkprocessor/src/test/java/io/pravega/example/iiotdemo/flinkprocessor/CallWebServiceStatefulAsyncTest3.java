package io.pravega.example.iiotdemo.flinkprocessor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.asynchttpclient.Dsl.asyncHttpClient;

// This demonstrates stateful AsyncIO. Unfortunately this is not supported as of Flink 1.5 so it doesn't work.
public class CallWebServiceStatefulAsyncTest3 {
    private static Logger log = LoggerFactory.getLogger(CallWebServiceStatefulAsyncTest3.class);

    @Test
    public void Test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<InputData> list = new ArrayList<>();
        for (int i = 0 ; i < 10 ; i++) {
            InputData inputData = new InputData();
            inputData.timestamp = i;
            inputData.device_id = String.format("%010d", i);
            list.add(inputData);
        }
        DataStream<InputData> ds = env.fromCollection(list);
        AsyncDataStream.unorderedWait(ds, new AsyncFunc(), 10000, TimeUnit.MILLISECONDS, 100).printToErr();
        env.execute();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class InputData implements Serializable {
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
    private static class OutputData implements Serializable {
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
