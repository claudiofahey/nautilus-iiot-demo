package io.pravega.example.pravega_gateway;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteBufferSerializer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server that manages startup/shutdown of a {@code PravegaGateway} server.
 */
public class PravegaGateway {
    private static final Logger logger = Logger.getLogger(PravegaGateway.class.getName());

    private Server server;

    private void start() throws IOException {
        int port = Parameters.getListenPort();
        server = ServerBuilder.forPort(port)
                .addService(new PravegaServerImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                PravegaGateway.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final PravegaGateway server = new PravegaGateway();
        server.start();
        server.blockUntilShutdown();
    }

    static class PravegaServerImpl extends PravegaGatewayGrpc.PravegaGatewayImplBase {

        @Override
        public void createScope(CreateScopeRequest req, StreamObserver<CreateScopeResponse> responseObserver) {
            StreamManager streamManager = StreamManager.create(Parameters.getControllerURI());
            boolean created = streamManager.createScope(req.getScope());
            responseObserver.onNext(CreateScopeResponse.newBuilder().setCreated(created).build());
            responseObserver.onCompleted();
        }

        @Override
        public void createStream(CreateStreamRequest req, StreamObserver<CreateStreamResponse> responseObserver) {
            StreamManager streamManager = StreamManager.create(Parameters.getControllerURI());
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            boolean created = streamManager.createStream(req.getScope(), req.getStream(), streamConfig);
            responseObserver.onNext(CreateStreamResponse.newBuilder().setCreated(created).build());
            responseObserver.onCompleted();
        }

        @Override
        public void readEvents(ReadEventsRequest req, StreamObserver<ReadEventsResponse> responseObserver) {
            final URI controllerURI = Parameters.getControllerURI();
            final String scope = req.getScope();
            final String streamName = req.getStream();

            final String readerGroup = UUID.randomUUID().toString().replace("-", "");
            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, streamName))
                    .build();
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
                readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            }

            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamReader<ByteBuffer> reader = clientFactory.createReader("reader",
                         readerGroup,
                         new ByteBufferSerializer(),
                         ReaderConfig.builder().build())) {
                for (;;) {
                    try {
                        // TODO: must be able to handle cancellation from client even if there are no events coming in. Perhaps reduce below timeout to 1 sec.
                        EventRead<ByteBuffer> event = reader.readNextEvent(req.getTimeoutMs());
                        if (event.isCheckpoint()) {
                            ReadEventsResponse response = ReadEventsResponse.newBuilder()
                                    .setCheckpointName(event.getCheckpointName())
                                    .build();
                            logger.info("readEvents: response=" + response.toString());
                            responseObserver.onNext(response);
                        } else if (event.getEvent() != null) {
                            ReadEventsResponse response = ReadEventsResponse.newBuilder()
                                    .setEvent(ByteString.copyFrom(event.getEvent()))
                                    .setPosition(event.getPosition().toString())
                                    .setEventPointer(event.getEventPointer().toString())
                                    .build();
                            logger.info("readEvents: response=" + response.toString());
                            responseObserver.onNext(response);
                        } else {
                            // If this is a bounded stream with an end stream cut, then we
                            // have reached the end stream cut.
                            // If this is an unbounded stream, all events have been read and a
                            // timeout has occurred.
                            logger.info("readEvents: no more events, completing RPC");
                            break;
                        }

                        if (Context.current().isCancelled()) {
                            logger.warning("context cancelled");
                            responseObserver.onError(Status.CANCELLED.asRuntimeException());
                            return;
                        }
                    } catch (ReinitializationRequiredException e) {
                        // There are certain circumstances where the reader needs to be reinitialized
                        logger.warning(e.toString());
                        responseObserver.onError(e);
                        return;
                    }
                }
            }

            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<WriteEventsRequest> writeEvents(StreamObserver<WriteEventsResponse> responseObserver) {
            return new StreamObserver<WriteEventsRequest>() {
                ClientFactory clientFactory;
                EventStreamWriter<ByteBuffer> writer;

                @Override
                public void onNext(WriteEventsRequest req) {
                    logger.info("writeEvents: req=" + req.toString());
                    if (writer == null) {
                        // The scope and stream are set based on the first request only.
                        // TODO: Check or remove this restriction.
                        final URI controllerURI = Parameters.getControllerURI();
                        final String scope = req.getScope();
                        final String streamName = req.getStream();
                        clientFactory = ClientFactory.withScope(scope, controllerURI);
                        writer = clientFactory.createEventWriter(
                                streamName,
                                new ByteBufferSerializer(),
                                EventWriterConfig.builder().build());
                    }
                    final CompletableFuture writeFuture = writer.writeEvent(req.getRoutingKey(), req.getEvent().asReadOnlyByteBuffer());
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.WARNING, "Encountered error in writeEvents", t);
                    if (writer != null) {
                        writer.close();
                        writer = null;
                    }
                    if (clientFactory != null) {
                        clientFactory.close();
                        clientFactory = null;
                    }
                }

                @Override
                public void onCompleted() {
                    if (writer != null) {
                        // Flush and close writer.
                        writer.close();
                        writer = null;
                    }
                    if (clientFactory != null) {
                        clientFactory.close();
                        clientFactory = null;
                    }
                    WriteEventsResponse response = WriteEventsResponse.newBuilder()
                            .build();
                    logger.info("writeEvents: response=" + response.toString());
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
