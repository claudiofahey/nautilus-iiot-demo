package io.pravega.example.iiotdemo.flinkprocessor;

public class ChunkSequenceException extends RuntimeException {
    public ChunkSequenceException(String s) {
        super(s);
    }
}
