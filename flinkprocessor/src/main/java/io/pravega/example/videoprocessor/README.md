
# Storing and Processing Video in Pravega with Flink

## Overview

The Pravega Event API is limited to 1 MiB events. 
To allow storage of objects larger than 1 MiB, such as video frames and images,
objects must be split into chunks of 1 MiB or less.

The logical object that is generally stored for video is the video frame.
In this example, the [VideoFrame](VideoFrame.java) class stores a video frame.
It has a timestamp and a byte array containing a PNG-encoded image.
The size of the image is limited only by the JVM which limits arrays to 2 GiB.

Note: If you are storing a video **transport stream**, such as Real-time Transport Protocol, 
HTTP Live Streaming, or MPEG Transport Stream (broadcast video), these are already packetized into
small packets of 188 or 1500 bytes. These can be stored in Pravega as-is. However, due to the complexity
involved in decoding such transport streams, it is often easier to store individual video frames
as ordinary images as described in this project.

## Writing Video to Pravega

Before a `VideoFrame` instance can be written to Pravega, it is split into
one or more [ChunkedVideoFrame](ChunkedVideoFrame.java) instances.
A `ChunkedVideoFrame` is a subclass of `VideoFrame` and it adds two 16-bit integers,
`ChunkIndex` and `FinalChunkIndex`.
`ChunkIndex` is a 0-based counter for this `ChunkedVideoFrame` within the `VideoFrame`.
`FinalChunkIndex` is the value of the final/maximum `ChunkIndex` and is equal to the number
of chunks minus 1.
For example, a 1.5 MiB image can be split into three 0.5 MiB chunks with {ChunkIndex, FinalChunkIndex} pairs of
{0,2}, {1,2}, {2,2}.

As implemented in `VideoFrameChunker`, the first chunk will contain the first 0.5 MB of the image, 
the second chunk will contain the second 0.5 MB of the image,
and so on. The last chunk may be smaller than the other chunks.

`ChunkedVideoFrame` instances are then serialized into JSON using `ChunkedVideoFrameSerializationSchema` and
it is this JSON that is written to the Pravega stream.
JSON is widely supported, simple to use, and easy to inspect.
However, because it requires base-64 encoding for byte arrays, it has a 33% storage overhead
compared to more efficient encodings such as Avro and Protobuf.

If a non-transactional Pravega writer were to fail while writing chunks of video, this could result in only some
of the chunks being written. Although this can easily be detected by the reassembly process, this could cause high
memory usage for the state of the reassembly process. To avoid this, Pravega transactions can be used to keep
chunks for the same image within a single transaction.

For an example of a Flink video writer job, see [VideoDataGeneratorJob](VideoDataGeneratorJob.java).

## Reading Video from Pravega

To read video frames from Pravega, the Pravega reader first reads the JSON-encoded `ChunkedVideoFrame`
and deserializes it.

Next, a series of Flink operations is performed.
```java
DataStream<VideoFrame> videoFrames = chunkedVideoFrames
        .keyBy("camera", "ssrc", "timestamp", "frameNumber")
        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
        .trigger(new ChunkedVideoFrameTrigger())
        .process(new ChunkedVideoFrameReassembler());
```

The `keyBy` function defines the attributes of `ChunkedVideoFrame` that uniquely identify a single frame.
As new events are read from Pravega from multiple Flink tasks in parallel, events with identical
values of these attributes will be grouped together and handled by the same task.

The `window` function applies a **session** window to each group of `ChunkedVideoFrame`.
Each session window will automatically timeout after 10 seconds of inactivity. This means that if
a non-transactional Pravega writer wrote only 2 of 3 chunks, these chunks would be purged from the
Flink state after 10 seconds, thus freeing memory.

The `ChunkedVideoFrameTrigger` trigger function simply watches for
`ChunkedVideoFrame` instances where `ChunkIndex` equals `FinalChunkIndex`. 
When this occurs, it returns
`FIRE_AND_PURGE` which tells it to call the `process` function and then it purges the
video frame from the state. 
Under normal conditions, the session window 10-second timeout never occurs
because `ChunkedVideoFrameTrigger` purges the state immediately upon the final chunk being received.

The `ChunkedVideoFrameReassembler` process function concatenates the byte arrays from all `ChunkedVideoFrame` instances
and outputs `VideoFrame` instances.
It checks for missing chunks and out-of-order chunks. 
It also validates that the SHA-1 hash of the data matches the hash calculated when it was written to Pravega.
Note that this check can be removed for high-throughput applications as Pravega and Flink
have additional layers of data consistency checks.

For an example of a Flink video reader job, see [VideoReaderJob](VideoReaderJob.java).

For a complete job that reads video from Pravega, processes it, and writes the processed video,
see [MultiVideoGridJob](MultiVideoGridJob.java).

## Running the Examples

Run the Flink video data generator job using the following parameters:
```
--jobClass
io.pravega.example.videoprocessor.VideoDataGeneratorJob
--controller
tcp://127.0.0.1:9090
--output-minNumSegments
6
--output-stream
examples/video1
```

Next, run a streaming Flink job that reads all video streams and combines them into a single video stream
where each image is composed of the input images in a square grid. 
Run the Flink app with the following parameters:
```
--jobClass
io.pravega.example.videoprocessor.MultiVideoGridJob
--controller
tcp://127.0.0.1:9090
--parallelism
2
--output-minNumSegments
6
--input-stream
examples/video1
--output-stream
examples/grid1
```

Run the Flink video reader job using the following parameters:
```
--jobClass
io.pravega.example.videoprocessor.VideoReaderJob
--controller
tcp://127.0.0.1:9090
--parallelism
2
--input-stream
examples/grid1
```
This will write a subset of images to `/tmp/camera*.png`.

## Memory Usage of the Flink Video Reader

A Flink task must store in memory all `ChunkedVideoFrame` instances which it has read until the final chunk for that
frame has been read or a session timeout occurs. A single Flink task that is reading from multiple Pravega segments
may receive chunks from interleaved frames, this requiring multiple partial frames to be buffered. To avoid out-of-memory
errors, each Flink task should have enough memory to buffer its share of Pravega segments.
For example, if you have 12 Pravega segments and 3 Flink reader tasks, you should account for 4 frames to be buffered
for each Flink reader task.

If you are using a non-transactional writer, you should also account for additional frames to be buffered.
If interruptions are rare, a single additional frame should be sufficient.
