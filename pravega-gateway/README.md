# Pravega Gateway

This is a GRPC server that provides a gateway to a Pravega server.
It provides **limited** Pravega functionality to any environment that support GRPC, including Python.

Using a GRPC gateway is better than a REST gateway for the following reasons:

- GRPC streaming is used for reading and writing events. This allows the Pravega connection to remain open for the life
  of the streaming request. Within a streaming request, any number of read or write operations can be performed.
  In the case of writing, these can also be wrapped in a transaction.
- GRPC uses Protobuf for efficient serialization.
  REST/JSON requires base64 encoding of binary data which is less efficient.

# Run Gateway

```
../gradlew run
```

# Rebuild Python GRPC Stub for Pravega Gateway

This section is only needed if you make changes to the pravega.proto file.

This will build the Python files necessary to allow a Python application to call this gateway.

1. Install [Miniconda Python 3.7](https://docs.conda.io/en/latest/miniconda.html) or
[Anaconda Python 3.7](https://www.anaconda.com/distribution/#download-section).

2. Create Conda environment.
    ```
    ./create_conda_env.sh
    ```

3. Run Protobuf compiler.
    ```
    ./build_python.sh
    ```
