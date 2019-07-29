
# Nautilus Industrial IoT Demo

## Overview

This project demonstrates how to use several key features of Nautilus to perform data ingestion, real-time analytics,
and visualization on streaming Industrial Internet-Of-Things (IOT) data.

## Components

- Pravega: Pravega provides a new storage abstraction - a stream - for continuous and unbounded data. 
  A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.

  Pravega provides dynamic scaling that can increase and decrease parallelism to automatically respond
  to changes in the event rate.

  For more information, see <http://pravega.io>.

- Flink: Apache Flink® is an open-source stream processing framework for distributed, high-performing, always-available, and accurate data streaming applications.
  See <https://flink.apache.org> for more information.
  
- Apache Spark: This is a unified analytics engine for large-scale data processing.
  It allows writing analytics applications in Java, Scala, Python, R, and SQL.
  
  This project uses the Spark Connector for Pravega to allow Python/Spark (PySpark) applications
  to read and analyze data in Pravega streams.

- Streaming Data Generator: A streaming data generator has been
  created that can generate synthetic sensor and video data and send it to the [Pravega Gateway](pravega-gateway).
  These are Python scripts and are in the [streaming_data_generator](streaming_data_generator) directory.
  
- Spark Jobs: These are various PySpark applications that show how to read and process 
  sensor and video data from a Pravega stream.
  These are Python scripts and are in the [spark_processor](spark_processor) directory.
  
- Pravega Gateway: This is a GRPC server that provides a gateway to a Pravega server.
  It provides **limited** Pravega functionality to any environment that support GRPC, including Python.
  
  For more information, see [Pravega Gateway](pravega-gateway).
  
- Jupyter Notebook: This is an open-source web application that allows you to create and share
  documents that contain live code, equations, visualizations, video, and narrative text.
  
  For more information, see <https://jupyter.org/>.
  
- Kubernetes: Kubernetes (K8s) is an open-source system for automating deployment, scaling, and management
  of containerized applications.
  
  This demo contains scripts and code to deploy all components on Kubernetes.
     
  For more information, see <https://kubernetes.io/>.

- Docker: This demo uses Docker and Docker Compose to greatly simplify the deployment of the various
  components on Linux and/or Windows servers, desktops, or even laptops.
  
  For more information, see <https://en.wikipedia.org/wiki/Docker_(software)>.

## Building and Running the Demo

In the steps below, sections noted with **(Nautilus SDK Desktop)** should only be performed
in a Nautilus SDK Desktop in a Kubernetes deployment of Nautilus.
Sections noted with **(Local)** should only be performed in a local workstation deployment
of Pravega.

### Download this Repository

```
cd
git clone https://github.com/claudiofahey/nautilus-iiot-demo
cd nautilus-iiot-demo
```

### (Local) Install Operating System

Install Ubuntu 16.04 LTS. Other operating systems can also be used but the commands below have only been tested
on this version.

### (Local) Install Java 8

```
apt-get install openjdk-8-jdk
```

### (Local, Optional) Install IntelliJ

Install from <https://www.jetbrains.com/idea>.
Enable the Lombok plugin.
Enable Annotations (settings -> build, execution, deployment, -> compiler -> annotation processors).

### (Local) Install Docker and Docker Compose

See <https://docs.docker.com/install/linux/docker-ce/ubuntu/>
and <https://docs.docker.com/compose/install/>.

### (Local) Run Pravega

This will run a development instance of Pravega locally.
Note that the default *standalone* Pravega used for development is likely insufficient for testing video because
it stores all data in memory and quickly runs out of memory.
Using the procedure below, all data will be stored in a small HDFS cluster in Docker.

In the command below, replace x.x.x.x with the IP address of a local network interface such as eth0.

```
cd
git clone https://github.com/pravega/pravega
cd pravega
git checkout r0.4
cd docker/compose
export HOST_IP=x.x.x.x
docker-compose up -d
```

You can view the Pravega logs with `docker-compose logs --follow`.

You can view the stream files stored on HDFS with `docker-compose exec hdfs hdfs dfs -ls -h -R /`.

### (Nautilus SDK Desktop) Install the Pravega Credentials JAR File

Accessing Pravega on Nautilus requires authorization provided by a credentials jar file.
This file is available in your home directory.
Install it in your local Maven repository with the following steps.

```
sudo apt-get install maven
mvn install:install-file \
-Dfile=$HOME/pravega-keycloak-credentials-shadow.jar \
-DgroupId=io.pravega -DartifactId=pravega-keycloak-credentials \
-Dversion=0.4.0-2030.d99411b-0.0.1-020.26736d2 -Dpackaging=jar
```

Then edit the file `gradle.properties` to include the following line.

```
includePravegaCredentials=true
```

### Flink Large Video Demo

See [videoprocessor/README.md](flinkprocessor/src/main/java/io/pravega/example/videoprocessor/README.md).

### Installing the Pravega Spark Connectors

The Pravega Spark Connector is not currently available in standard repositories.
To install it, follow the procedure at
[Spark Connectors](https://github.com/pravega/spark-connectors).

### Build the Python Environments

1. Install [Miniconda Python 3.7](https://docs.conda.io/en/latest/miniconda.html).
   ```
   bash Miniconda3-latest-Linux-x86_64.sh
   ```
   To make the changes take effect, close and then re-open your Terminal window.

2. Create Conda environments.
    ```
    cd streaming_data_generator && \
    ./create_conda_env.sh && \
    cd ../spark_processor && \
    ./create_conda_env.sh
    ```

### Install Apache Spark

This will install a development instance of Spark locally.

Download [Apache Spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz).

```
mkdir -p ~/spark
cd ~/spark
tar -xzvf ~/Downloads/spark-2.4.1-bin-hadoop2.7.tgz
ln -s spark-2.4.1-bin-hadoop2.7 current
export PATH="$HOME/spark/current/bin:$PATH"
spark-submit --version
```

#### (Local, Optional) Start Spark Services

By default, the Spark applications will use an in-process Spark cluster.
You can also start a Spark cluster using the following command.

```
~/spark/current/sbin/start-all.sh
```

Confirm that you can browse to the Spark Master UI at http://localhost:8080/.

### Run the Pravega Gateway

(Nautilus SDK Desktop) Set the Pravega controller in Nautilus.
Skip this command if you have a local Pravega installation.
```
export PRAVEGA_CONTROLLER=tcp://nautilus-pravega-controller.nautilus-pravega.svc.cluster.local:9090
```

Run the Pravega Gateway.
```
./gradlew pravega-gateway:run
```

See [Pravega Gateway](pravega-gateway/README.md) for more information.

### Run the Data Generators

This will run Python applications that generate synthetic data and writes it to Pravega
via the Pravega Gateway. You may run any number of these and use a variety of options.
Run with the `--help` option.

```
cd streaming_data_generator
./sensor_data_generator.sh
./sensor_data_generator.sh --stream sensors2
./video_data_generator.sh
```

### Run the Flink Multi Video Grid Job.

This will run a streaming Flink job that reads all video streams and combines them into a single video stream
where each image is composed of the input images in a square grid.

Run the Flink app in `flinkprocessor` with the following parameters:
```
--jobClass
io.pravega.example.videoprocessor.MultiVideoGridJob
--controller
tcp://127.0.0.1:9090
--input-stream
examples/unchunkedvideo
--output-stream
examples/unchunkedcombinedvideo
```

### View Live Video in Jupyter Notebook

```
cd jupyterhub
export DOCKER_REPOSITORY=<hostname>:<port>/<namespace>
./build.sh
docker-compose up
```

Review the log produced above to find the Jupyter login token. You will need that to login to Jupyter.

Open your browser to <http://localhost:8888/notebooks/notebooks/test_play_video_from_pravega.ipynb>.
Click Kernel -> Restart and Run All.
Live (generated) video should begin to play.

### Run the Spark Video and Sensor Processors

This will run a Python Spark application that reads the generated data. 

Edit the file `spark_processor/run_spark_app.sh`.
Uncomment the lines following "Use below for Nautilus SDK Desktop" and comment the lines
following "Use below for local Pravega".

```
cd spark_processor
./test_video_and_sensor_processor.sh
```

Note that Spark checkpoints are stored in `/tmp/spark_checkpoints*`. These files are used to restart
failed jobs from the last successful checkpoint. If you change the Pravega stream or make significant
changes to the job, you should delete these files to prevent Spark from trying to use the checkpoint.

## Deploying to Nautilus

### Deploy Pravega Gateway to Kubernetes

Place pravega-keycloak-credentials-0.4.0-2030.d99411b-0.0.1-020.26736d2-shadow.jar in the lib directory.

If your network requires non-standard TLS certificates to be trusted during the build process, 
place them in the ca-certificates directory.

```
export DOCKER_REPOSITORY=<hostname>:<port>/<namespace>
export IMAGE_TAG=0.3.0
scripts/build-k8s-components.sh
scripts/deploy-k8s-components.sh
```

### Deploy Spark Processor to Kubernetes

```
export DOCKER_REPOSITORY=<hostname>:<port>/<namespace>
export IMAGE_TAG=0.3.0
spark_processor/build-k8s.sh
kubectl apply -f spark_processor/spark-service-account.yaml -n examples
spark_processor/submit-k8s.sh test_sensor_processor
spark_processor/submit-k8s.sh test_video_and_sensor_processor
```

# Notes

- Apache Flink jobs have been temporarily removed from this project until they are updated.
  However, the [v0.2 branch](https://github.com/claudiofahey/nautilus-iiot-demo/tree/r0.2) has the previous code.

# References

- <http://pravega.io/>

# Appendix
