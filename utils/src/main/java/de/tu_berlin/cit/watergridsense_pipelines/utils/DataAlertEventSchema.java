package de.tu_berlin.cit.watergridsense_pipelines.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DataAlertEventSchema implements DeserializationSchema<DataAlert>, SerializationSchema<DataAlert> {
    private static final Logger LOG = Logger.getLogger(DataAlertEventSchema.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerModule(new JodaModule());
    }

    private static DataAlert fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 3) {
            LOG.error("Invalid Record: " + line);
        }
        else {
            try {
                return MAPPER.readValue(line, DataAlert.class);
            }
            catch (IOException ex) {
                LOG.error("Mapping error: " + ex.getMessage());
            }
        }
        return null;
    }

    @Override
    public DataAlert deserialize(byte[] message) {
        return fromString(new String(message));
    }

    @Override
    public boolean isEndOfStream(DataAlert nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(DataAlert element) {
        return element.toString().getBytes();
    }

    @Override
    public TypeInformation<DataAlert> getProducedType() {
        return TypeExtractor.getForClass(DataAlert.class);
    }

}
