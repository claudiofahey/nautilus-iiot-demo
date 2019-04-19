
# Nautilus Industrial IoT Demo

## Overview

This project demonstrates how to use several key features of Nautilus to perform data ingestion, real-time analytics,
and visualization on streaming Industrial Internet-Of-Things (IOT) data.

## Components

- Pravega: Pravega provides a new storage abstraction - a stream - for continuous and unbounded data. 
  A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.

  Pravega provides dynamic scaling that can increase and decrease parallelism to automatically respond
  to changes in the event rate.

  See <http://pravega.io> for more information.

- Apache Spark

- Docker: This demo uses Docker and Docker Compose to greatly simplify the deployment of the various
  components on Linux and/or Windows servers, desktops, or even laptops.
  For more information, see <https://en.wikipedia.org/wiki/Docker_(software)>.

## Building and Running the Demo

### Install Operating System

Install Ubuntu 16.04 LTS. Other operating systems can also be used but the commands below have only been tested
on this version.

### Install Java 8

```
apt-get install openjdk-8-jdk
```

### (Optional) Install IntelliJ

Install from <https://www.jetbrains.com/idea>.
Enable the Lombok plugin. 
Enable Annotations (settings -> build, execution, deployment, -> compiler -> annotation processors).

### Install Docker and Docker Compose

See <https://docs.docker.com/install/linux/docker-ce/ubuntu/>
and <https://docs.docker.com/compose/install/>.

### Run Pravega

This will run a development instance of Pravega locally.
Note that the default *standalone* Pravega used for development is likely insufficient for testing video because
it stores all data in memory and quickly runs out of memory.
Using the procedure below, all data will be stored in a small HDFS cluster in Docker.

In the command below, replace x.x.x.x with the IP address of a local network interface such as eth0.

```
cd
git clone https://github.com/pravega/pravega
cd pravega
export HOST_IP=x.x.x.x
docker-compose up -d
```

You can view the Pravega logs with `docker-compose logs --follow`.

You can view the stream files stored on HDFS with `docker-compose exec hdfs hdfs dfs -ls -h -R /`.

### Run the Pravega Gateway

```
./gradlew pravega-gateway:run
```

See [Pravega Gateway](pravega-gateway/README.md) for more information.

### Build the Python Environment

1. Install [Miniconda Python 3.7](https://docs.conda.io/en/latest/miniconda.html) or
   [Anaconda Python 3.7](https://www.anaconda.com/distribution/#download-section).

2. Create Conda environment.
    ```
    ./create_conda_env.sh
    ```

### Run the Data Generator

This will run a Python application that generates random data and writes it to Pravega
via the Pravega Gateway.

```
./data_generator.sh
```

### Installing the Pravega Spark Connectors

The Pravega Spark Connector is not currently available in standard repositories.
To install it, follow the procedure at
[Spark Connectors](https://github.com/pravega/spark-connectors/tree/issue-11-chunked-reader#build-and-install-the-spark-connector).

### Run the Video Processor

This will run a Python Spark application that reads the random data. It can be modified to perform computation on the data.

```
./data_generator.sh
```

# References

- <http://pravega.io/>

# Appendix
