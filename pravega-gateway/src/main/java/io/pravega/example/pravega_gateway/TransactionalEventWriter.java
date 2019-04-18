package io.pravega.example.pravega_gateway;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;

import java.util.logging.Logger;

public class TransactionalEventWriter<T> extends AbstractEventWriter<T>  {
    private static final Logger logger = Logger.getLogger(TransactionalEventWriter.class.getName());

    /**
     * The currently running transaction to which we writeEvent
     */
    private Transaction<T> currentTxn;

    private EventStreamWriter<T> pravegaWriter;

    public TransactionalEventWriter(EventStreamWriter<T> pravegaWriter) {
        this.pravegaWriter = pravegaWriter;
    }

    @Override
    void open() {
        currentTxn = pravegaWriter.beginTxn();
        logger.info("open: began transaction " + currentTxn.getTxnId());
    }

    @Override
    void writeEvent(String routingKey, T event) throws TxnFailedException {
        currentTxn.writeEvent(routingKey, event);
    }

    @Override
    void close() throws TxnFailedException {
        logger.info("close: committing transaction " + currentTxn.getTxnId());
        currentTxn.commit();
        pravegaWriter.close();
    }
}
