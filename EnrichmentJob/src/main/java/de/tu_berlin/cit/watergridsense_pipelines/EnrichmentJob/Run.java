package de.tu_berlin.cit.watergridsense_pipelines.EnrichmentJob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.sql.Timestamp;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisServerCommands;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import de.tu_berlin.cit.watergridsense_pipelines.utils.Cassandra;
import de.tu_berlin.cit.watergridsense_pipelines.utils.FileReader;
import de.tu_berlin.cit.watergridsense_pipelines.utils.SensorData;
import de.tu_berlin.cit.watergridsense_pipelines.utils.ParamData;
import de.tu_berlin.cit.watergridsense_pipelines.utils.SensorDataEventSchema;
import de.tu_berlin.cit.watergridsense_pipelines.utils.SensorDataTSExtractor;
import de.tu_berlin.cit.watergridsense_pipelines.utils.ParamDataEventSchema;
import de.tu_berlin.cit.watergridsense_pipelines.utils.ParamDataTSExtractor;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(); // configure logger
        Properties props = FileReader.GET.read("enrichment_job.properties", Properties.class);
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String cassandraHosts = props.getProperty("cassandra.hosts");
        int cassandraPort = Integer.parseInt(props.getProperty("cassandra.port"));
        String keySpace = props.getProperty("cassandra.keyspace");
        String username = props.getProperty("cassandra.user");
        String password = props.getProperty("cassandra.password");

        // setup cassandra cluster builder for keyspace creation
        ClusterBuilder builder = new ClusterBuilder() {

            @Override
            protected Cluster buildCluster(Builder builder) {
                return builder
                    .addContactPoints(cassandraHosts.split(","))
                    .withPort(cassandraPort)
                    .withCredentials(username, password)
                    .build();
            }
        };
        
        // Initialize Cassandra ****************************************************************

        // program parameter "initialize" creates keyspace and table
        if (parameters.has("initialize")) {
            Cassandra.execute(
            "CREATE KEYSPACE IF NOT EXISTS " + keySpace + " WITH replication = {" +
                    "'class':'SimpleStrategy', " +
                    "'replication_factor':3};",
                builder);

            // setup cassandra
            Cassandra.execute(
                "CREATE TABLE IF NOT EXISTS " + keySpace + ".parameters(" +
                        "sensorid BIGINT, " +
                        "timestamp TIMESTAMP, " +
                        "parameter TEXT, " +
                        "value TEXT, " +
                        "PRIMARY KEY (sensorid, parameter, timestamp)) " +
                        "WITH CLUSTERING ORDER BY (parameter ASC, timestamp DESC);",
                builder);
        }

        // Configure connections ****************************************************************

        RMQConnectionConfig connectionConfig = null;
        if (parameters.has("mqtt")) {
            // rabbitmq configuration
            connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(props.getProperty("rabbitmq.host"))
                .setPort(Integer.parseInt(props.getProperty("rabbitmq.amqp.port")))
                .setVirtualHost(props.getProperty("rabbitmq.vhost"))
                .setUserName(props.getProperty("rabbitmq.user"))
                .setPassword(props.getProperty("rabbitmq.password"))
                .build();
        }
        // kafka configuration
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaConsumerProps.setProperty("group.id", props.getProperty("kafka.consumer.group"));   // Consumer group ID
        if (parameters.has("latest")) {
            kafkaConsumerProps.setProperty("auto.offset.reset", "latest");                          
        }
        else {
            kafkaConsumerProps.setProperty("auto.offset.reset", "earliest");
        }
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", props.getProperty("kafka.brokers")); // Broker default host:port
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"900000");
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"my-transaction");
        kafkaProducerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Configure Flink ****************************************************************

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        // setup RocksDB state backend
        String fileBackend = props.getProperty("hdfs.host") + props.getProperty("hdfs.path");
        RocksDBStateBackend backend = new RocksDBStateBackend(fileBackend, true);
        env.setStateBackend((StateBackend) backend);

        // start a checkpoint based on configuration property
        int checkpointInterval = Integer.parseInt(props.getProperty("flink.checkpointInterval"));
        env.enableCheckpointing(checkpointInterval);

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(300000);

        // enable externalized checkpoints which are deleted after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // allow job recovery fallback to checkpoint when there is a more recent savepoint
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // configure event-time and watermarks
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // Setup input streams ****************************************************************

        DataStream<SensorData> rawSensorDataStream;
        
        // program parameter "mqtt" selects RabbitMQ instead of kafka
        if (parameters.has("mqtt")) {
            // setup RabbitMQ measurement consumer
            final RMQSource<SensorData> rawMqttConsumer = 
                new RMQSource<SensorData>(
                    connectionConfig,            // config for the RabbitMQ connection
                    props.getProperty("rabbitmq.topic.raw"), // name of the RabbitMQ queue to consume
                    false,                        // use correlation ids; true == exactly-one, false == at-least-once
                    new SensorDataEventSchema());   // deserialization schema to turn messages into Java objects
            
            // rabbitmq stream for raw measurements
            rawSensorDataStream = env
                    .addSource(rawMqttConsumer)
                    .name("MQTT sensor data consumer")
                    .setParallelism(1)  // with RabbitMQ, all we get is "at least once" semantics
                    .keyBy(SensorData::getSensorId);
        }
        else {
            // create kafka sensor measurement stream
            FlinkKafkaConsumer<SensorData> rawConsumer =
                new FlinkKafkaConsumer<>(
                    props.getProperty("kafka.topic.raw"),
                    new SensorDataEventSchema(),
                    kafkaConsumerProps);
            rawConsumer.assignTimestampsAndWatermarks(new SensorDataTSExtractor(Time.seconds(60)));
            rawSensorDataStream = env
                .addSource(rawConsumer)
                .name("Kafka sensor data consumer")
                .keyBy(SensorData::getSensorId);
        }

        // create kafka parameter update stream
        FlinkKafkaConsumer<ParamData> paramUpdateConsumer =
            new FlinkKafkaConsumer<>(
                props.getProperty("kafka.topic.updates"),
                new ParamDataEventSchema(),
                kafkaConsumerProps);
        paramUpdateConsumer.assignTimestampsAndWatermarks(new ParamDataTSExtractor(Time.seconds(60)));
        DataStream<ParamData> paramUpdateDataStream = env
            .addSource(paramUpdateConsumer)
            .name("Kafka parameter update consumer")
            .keyBy(ParamData::getSensorId);

        // Enrichment ****************************************************************

        // connect and enrich sensor data with parameter data
        DataStream<SensorData> enrichedSensorDataStream = rawSensorDataStream
            .connect(paramUpdateDataStream)
            .flatMap(new EnrichmentFunction())
            .name("Enrichment Function");

        // Write to sinks ****************************************************************
        
        FlinkKafkaProducer<SensorData> richProducer =
            new FlinkKafkaProducer<>(
                props.getProperty("kafka.topic.rich"),
                (KafkaSerializationSchema<SensorData>) (sensorData, aLong) -> {
                    return new ProducerRecord<>(props.getProperty("kafka.topic.rich"), sensorData.toString().getBytes());
                },
                kafkaProducerProps,
                Semantic.EXACTLY_ONCE);

        // write back to kafka output topic
        enrichedSensorDataStream
            .addSink(richProducer)
            .name("Kafka enriched sensor data sink");

        // store updates in `parameters` table
        // Note: Cassandra sinks have parallelism=1 to avoid session exhaustion
        DataStream<Tuple4<Long, Date, String, String>> archiveParamUpdateStream =
            paramUpdateDataStream
            .map((MapFunction<ParamData, Tuple4<Long, Date, String, String>>) ParamData::toTuple)
            .setParallelism(1)
            .name("Cassandra archive parameters");
        CassandraSink.addSink(archiveParamUpdateStream)
            .setClusterBuilder(builder)
            .setQuery(
                "INSERT INTO " + keySpace + ".parameters(sensorid, timestamp, parameter, value) " +
                "VALUES (?, ?, ?, ?);")
            .build()
            .setParallelism(1);    

        env.execute("WGS4_EnrichmentJob");
    }
}
