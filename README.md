# WaterGridSense4.0 Analytics Platform Enrichment Job

This repository contains the [Apache Flink](https://flink.apache.org/) job used to enrich the sensor data stream as part of the [WaterGridSense4.0](https://www.dos.tu-berlin.de/menue/research/watergridsense_40/) Analytics Platform. 

## Requirements

This job requires a running instance of our [Analytics Platform](https://github.com/dos-group/water-analytics-cluster) and a running HDFS cluster.

## Configuration

The job is configured by adjusting the settings in the properties file `src/main/resources/enrichment_job.properties`. Note that all properties with capitalized values **must** be changed, e.g. replace `KAFKA_HOST_OR_IP`, `RABBITMQ_PASSWORD` etc. with the correct values for your cluster.

## Building

To build the pipelines, just use `make`:

```
make
```

or use maven directly instead

```
mvn clean compile package
```

## Running

Submit the job to flink. Use the `Parallelism` setting to configure the cluster size in accordance with the number of Flink taskmanagers and Kafka brokers. Other parameters that can be passed to the job are:

- `--initialize` - initialize the Cassandra DB by creating the keyspace and parameter table
- `--mqtt` - use RabbitMQ as data stream broker instead of Kafka