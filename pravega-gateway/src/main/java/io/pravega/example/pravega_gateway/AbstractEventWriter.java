package io.pravega.example.pravega_gateway;

import io.pravega.client.stream.TxnFailedException;

abstract class AbstractEventWriter<T> {
    abstract void open();

    abstract void writeEvent(String routingKey, T event) throws TxnFailedException;

    abstract void close() throws TxnFailedException;
}
