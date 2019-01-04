package com.dell.mqtt.pravega;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class PravegaHelper {

    private static Logger log = LoggerFactory.getLogger( PravegaHelper.class );

    public static EventStreamWriter<DataPacket> getStreamWriter(ApplicationArguments.PravegaArgs pravegaArgs) {

        log.info("Connecting to Pravega URI: {}, Scope: {}, Stream: {}",
                pravegaArgs.controllerUri, pravegaArgs.scope, pravegaArgs.stream);

        StreamManager streamManager = StreamManager.create(URI.create(pravegaArgs.controllerUri));

        final boolean scopeIsNew = streamManager.createScope(pravegaArgs.scope);
        if (!scopeIsNew) {
            log.info("Scope: {} is already available", pravegaArgs.scope);
        }

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(pravegaArgs.targetRate, pravegaArgs.scaleFactor, pravegaArgs.minNumSegments))
                .build();

        final boolean streamIsNew = streamManager.createStream(pravegaArgs.scope, pravegaArgs.stream, streamConfig);
        if (!streamIsNew) {
            log.info("Stream: {} is already available", pravegaArgs.stream);
        }

        ClientFactory clientFactory = ClientFactory.withScope(pravegaArgs.scope, URI.create(pravegaArgs.controllerUri));
        EventStreamWriter<DataPacket> writer = clientFactory.createEventWriter(pravegaArgs.stream,
                new JavaSerializer<DataPacket>(),
                EventWriterConfig.builder().build());

        return writer;

    }
}
