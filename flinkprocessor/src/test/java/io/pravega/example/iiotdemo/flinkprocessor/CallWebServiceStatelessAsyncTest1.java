package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.asynchttpclient.Dsl.asyncHttpClient;

@Ignore()
public class CallWebServiceStatelessAsyncTest1 {
    private static Logger log = LoggerFactory.getLogger(CallWebServiceStatelessAsyncTest1.class);

    @Test
    public void Test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<String> list = new ArrayList<>();
        for (int i = 0 ; i < 100 ; i++) {
            list.add(String.format("%010d", i));
        }
        DataStream<String> ds = env.fromCollection(list);
        AsyncDataStream.unorderedWait(ds, new AsyncFunc(), 10000, TimeUnit.MILLISECONDS, 100).printToErr();
        env.execute();
    }

    static class AsyncFunc extends RichAsyncFunction<String, String> {
        transient AsyncHttpClient asyncHttpClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            asyncHttpClient = asyncHttpClient();
        }

        @Override
        public void close() throws Exception {
            asyncHttpClient.close();
        }

        @Override
        public void asyncInvoke(final String input, final ResultFuture<String> resultFuture) throws Exception {
//            String url = "http://httpbin.org/post";
            String url = "http://localhost:8123/post";
//            String url = "http://localhost:5001/predict";
            asyncHttpClient
                    .preparePost(url)
                    .setBody(input)
                    .execute()
                    .toCompletableFuture()
                    .thenApply(Response::getResponseBody)
                    .thenAccept((String result) -> {
                        resultFuture.complete(Collections.singleton(result));
                    });
        }
    }
}
