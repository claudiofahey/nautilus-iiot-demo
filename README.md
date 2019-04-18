
# Nautilus Industrial IoT Demo

## Overview

This projects demonstrates how to use several key features of Nautilus to perform real-time analytics
and visualization on streaming Internet-Of-Things (IOT) data.

## Components

- Pravega: Pravega provides a new storage abstraction - a stream - for continuous and unbounded data. 
  A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.

  Pravega provides dynamic scaling that can increase and decrease parallelism to automatically respond
  to changes in the event rate.

  See <http://pravega.io> for more information.

- Flink: Apache Flink® is an open-source stream processing framework for distributed, high-performing, always-available, and accurate data streaming applications.
  See <https://flink.apache.org> for more information.
  
- Flink Streaming Job: This is a Java application that defines a job that can be executed in the Flink cluster.
  This particular job simply reads the events from Pravega and loads them into Elasticsearch for visualization.
  This is in the [flinkprocessor](flinkprocessor) directory. 
  
- Docker: This demo uses Docker and Docker Compose to greatly simplify the deployment of the various
  components on Linux and/or Windows servers, desktops, or even laptops.
  For more information, see <https://en.wikipedia.org/wiki/Docker_(software)>.

## Building and Running the Demo

### Install Operating System

Install Ubuntu 16.04 LTS. Other operating systems can also be used but the commands below have only been tested
on this version.

### Install Java

```
apt-get install default-jdk
```

### Install IntelliJ

Install from <https://www.jetbrains.com/idea>.
Enable the Lombok plugin. 
Enable Annotations (settings -> build, execution, deployment, -> compiler -> annotation processors).

### Install Docker and Docker Compose

See <https://docs.docker.com/install/linux/docker-ce/ubuntu/>
and <https://docs.docker.com/compose/install/>.

### Run Flink Jobs

- Flink jobs are in the [flinkprocessor](flinkprocessor) directory.

- Build the uber jar `flinkprocessor/build/libs/iiotdemo-flinkprocessor-0.2.0-SNAPSHOT-all.jar`:
```
./gradlew flinkprocessor:build
```

- The Flink jobs can be executed with Nautilus, another Flink cluster, or in standalone mode.  
  In standalone mode, a mini Flink cluster will execute within the application process.
  When run in the IntelliJ IDE, standalone mode will be used by default.

### Run Flink Jobs outside Nautilus

- When Flink jobs are run outside of Nautilus (e.g. in Intellij) and that need to connect to Pravega
  running in Nautilus, you must set the following authentication environment variables:
  - pravega_client_auth_loadDynamic=true
  - pravega_client_auth_method=nautilus
  - NAUTILUS_GUARDIAN_URL=https://${nautilusMasterIP}/auth
  - NAUTILUS_USERNAME=${username}
  - NAUTILUS_PASSWORD=${password}

- Stream the raw data to the console (stderr):
```
--jobClass io.pravega.example.iiotdemo.flinkprocessor.StreamToConsoleJob
--controller tcp://$PRAVEGA_HOST_ADDRESS:9090 
--input-stream iot/data 
```

- Stream the raw data to Elasticsearch:
```
--jobClass io.pravega.example.iiotdemo.flinkprocessor.StreamRawDataToElasticsearchJob
--controller tcp://$PRAVEGA_HOST_ADDRESS:9090 
--input-stream iot/data 
--elastic-sink true 
--elastic-host $ELASTIC_HOST 
--elastic-delete-index true
```

- Extract statistics and write to Elasticsearch:
```
--jobClass io.pravega.example.iiotdemo.flinkprocessor.StreamStatisticsToElasticsearchJob
--controller tcp://$PRAVEGA_HOST_ADDRESS:9090 
--input-stream iot/data 
--elastic-sink true 
--elastic-host $ELASTIC_HOST 
--elastic-delete-index true
```

### Run Flink Jobs in Nautilus using the Nautilus UI

1. Nautilus -> Analytics -> Apps -> (+).
2. Name: StreamToConsoleJob
3. Artifact: io.pravega.example.iiotdemo:iiotdemo-flinkprocessor:0.2.0-SNAPSHOT
4. Configuration:
   Parallelism: 1
   Property: jobClass: io.pravega.example.iiotdemo.flinkprocessor.StreamToConsoleJob
   Stream: input-stream: iot/data 
5. Click Launch.

### Run Flink Jobs in Nautilus using the Flink UI

- Stream the raw data to the console (stderr):
  - Program Arguments:
```
--jobClass io.pravega.example.iiotdemo.flinkprocessor.StreamToConsoleJob
--controller tcp://controller.pravega.l4lb.thisdcos.directory:9091
--input-stream iot/data 
```

- Stream the raw data to Elasticsearch:
  - Program Arguments:
```
--jobClass io.pravega.example.iiotdemo.flinkprocessor.StreamRawDataToElasticsearchJob
--controller tcp://controller.pravega.l4lb.thisdcos.directory:9091
--input-stream iot/data 
--elastic-sink true 
--elastic-host $ELASTIC_HOST 
--elastic-delete-index true
```

# References

- <http://pravega.io/>
- <http://pravega.io/docs/latest/deployment/run-local/>
- <https://flink.apache.org>
- <https://cwiki.apache.org/confluence/display/FLINK/Streams+and+Operations+on+Streams>
- <https://jersey.java.net/documentation/latest/getting-started.html>
- <http://www.oracle.com/webfolder/technetwork/tutorials/obe/java/griz_jersey_intro/Grizzly-Jersey-Intro.html>
- <https://github.com/deviantony/docker-elk>

# Appendix

### Build Pravega and Connectors (optional)

These steps are only required if you want to use the latest version of the Pravega dependencies.

```
cd pravega
./gradlew install 
./gradlew startStandalone

cd flink-connectors
./gradlew install

cd hadoop-connectors
./gradlew install
```
