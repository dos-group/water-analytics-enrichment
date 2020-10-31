package de.tu_berlin.cit.watergridsense_pipelines.EnrichmentJob;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisHashCommands;
import de.tu_berlin.cit.watergridsense_pipelines.utils.Cassandra;
import de.tu_berlin.cit.watergridsense_pipelines.utils.FileReader;
import de.tu_berlin.cit.watergridsense_pipelines.utils.ParamCache;
import de.tu_berlin.cit.watergridsense_pipelines.utils.SensorData;
import de.tu_berlin.cit.watergridsense_pipelines.utils.ParamData;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class EnrichmentFunction extends RichCoFlatMapFunction<SensorData, ParamData, SensorData> {

    private Map<String, ParamCache> paramState;
    private static final Logger LOG = Logger.getLogger(EnrichmentFunction.class);
    private transient Properties props;
    private transient RedisClient redisClient;
    private transient StatefulRedisConnection<String, String> redisConnection;
    private transient RedisHashCommands<String, String> redisHashCommands;

    @Override
    public void open(final Configuration config) throws Exception {
        // configure logger
        BasicConfigurator.configure();
        props = FileReader.GET.read("water_analytics_job.properties", Properties.class);
        // connect to redis
        RedisURI redisUri = RedisURI.builder()
                .withHost(props.getProperty("redis.host"))
                .withPort(Integer.valueOf(props.getProperty("redis.port")))
                .build();
        redisClient = RedisClient.create(redisUri);
        redisConnection = redisClient.connect();
        redisHashCommands = redisConnection.sync();
        paramState = new HashMap<>();
    }
    
    @Override
    public void close() throws Exception {
        redisConnection.close();
        redisClient.shutdown();
        super.close();
    }

    @Override
    public void flatMap1(SensorData sensorData, Collector<SensorData> out) throws Exception {
        long sensorid = sensorData.getSensorId();
        SensorData enrichedSensorData = new SensorData(sensorid,
                sensorData.timestamp,
                sensorData.rawValue);

        // warm up cache with value from redis
        if(paramState.isEmpty()){
            try {
                Map<String, String> parameters = redisHashCommands.hgetall(Long.toString(sensorid));
                for (Map.Entry<String, String> entry : parameters.entrySet()) {
                    String parameterName = entry.getKey();
                    String[] parameterValue = entry.getValue().split("@");
                    ParamCache paramCache = new ParamCache();
                    paramCache.current = new Tuple2<>(new Date(Long.valueOf(parameterValue[1])), parameterValue[0]);
                    paramState.put(parameterName, paramCache);
                }
            }
            catch (Exception e) {
                LOG.error(e);
            }
        }
        
        for (String parameter : paramState.keySet()) {
            String parameterValue = "NOPARAM";
            // check if current value of paramCache is correct
            if (paramState.get(parameter).current.f0.before(sensorData.timestamp)) {
                parameterValue = paramState.get(parameter).current.f1;
            }
            // in almost all cases, the previous one will be the correct one
            else if (paramState.get(parameter).previous != null &&
                    paramState.get(parameter).previous.f0.before(sensorData.timestamp)) {
                parameterValue = paramState.get(parameter).previous.f1;
            }
            // in the worst case, we have to kindly ask cassandra to give us the correct value
            else {
                try {
                    ClusterBuilder builder = new ClusterBuilder() {
                        @Override
                        protected Cluster buildCluster(Builder builder) {
                            return builder
                                .addContactPoints(props.getProperty("cassandra.hosts").split(","))
                                .withPort(Integer.parseInt(props.getProperty("cassandra.port")))
                                .withCredentials(props.getProperty("cassandra.user"), props.getProperty("cassandra.password"))
                                .build();
                        }
                    };

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String queryString = "SELECT value FROM " +
                            props.getProperty("cassandra.keyspace") +
                            ".parameters WHERE sensorid=" + sensorid +
                            " AND parameter='" + parameter +
                            "' AND timestamp < '" + simpleDateFormat.format(sensorData.timestamp) + 
                            "' LIMIT 1;";
                    ResultSet result = Cassandra.execute(queryString, builder);
                    Row row = result.one();
                    if (row != null) {
                        parameterValue = row.getString("value");
                        Cassandra.close();
                    }
                    else {
                        LOG.error("No parameter found for sensor with ID "+sensorid+" and timestamp "+sensorData.timestamp.toString());
                    }
                }
                catch (NoHostAvailableException e) {
                    for (Throwable t : e.getErrors().values()) {
                        t.printStackTrace();
                    }
                }
            }

            switch (parameter) {
                case "geolocation":
                    enrichedSensorData.location = parameterValue;
                    break;
                case "type":
                    enrichedSensorData.type = parameterValue;
                    break;
                case "unit":
                    enrichedSensorData.unit = parameterValue;
                    break;
                case "conversion":
                    if (parameterValue != "NOPARAM") {
                        enrichedSensorData.conValue = enrichedSensorData.rawValue * Double.valueOf(parameterValue);
                    }
                    else {
                        enrichedSensorData.conValue = -1.;
                    }
                    break;
            }
        }
        out.collect(enrichedSensorData);
    }

    @Override
    public void flatMap2(ParamData paramData, Collector<SensorData> out) throws Exception {
        ParamCache cachedParam = paramState.get(paramData.parameter);
        if(cachedParam == null) {
            cachedParam = new ParamCache();
        }
        // old "current" value is now "previous"
        cachedParam.previous = cachedParam.current;
        // set new "current" value to received parameter
        cachedParam.current = new Tuple2<>(paramData.timestamp, paramData.value);
        paramState.put(paramData.parameter, cachedParam);

        // update redis
        try {
            redisHashCommands.hset(
                Long.toString(paramData.sensorId),
                paramData.parameter,
                paramData.value + "@" + paramData.timestamp.getTime());
        }
        catch (Exception e) {
            LOG.error(e);
        }
    }
}