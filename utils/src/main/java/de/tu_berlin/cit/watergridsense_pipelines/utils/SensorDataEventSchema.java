package de.tu_berlin.cit.watergridsense_pipelines.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SensorDataEventSchema implements DeserializationSchema<SensorData>, SerializationSchema<SensorData> {

    private static final Logger LOG = Logger.getLogger(SensorDataEventSchema.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerModule(new JodaModule());
    }

    private static SensorData fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 3) {
            LOG.error("Invalid Record: " + line);
        }
        else {
            try {
                return MAPPER.readValue(line, SensorData.class);
            }
            catch (IOException ex) {
                LOG.error("Mapping error: " + ex.getMessage());
            }
        }
        return null;
    }

    @Override
    public SensorData deserialize(byte[] message) {
        return fromString(new String(message));
    }

    @Override
    public boolean isEndOfStream(SensorData nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(SensorData element) {
        return element.toString().getBytes();
    }

    @Override
    public TypeInformation<SensorData> getProducedType() {
        return TypeExtractor.getForClass(SensorData.class);
    }
}

