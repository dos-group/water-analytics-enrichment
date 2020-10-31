package de.tu_berlin.cit.watergridsense_pipelines.utils;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.api.java.tuple.Tuple7;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class SensorData {

    private static class UnixTimestampDeserializer extends JsonDeserializer<Date> {

        @Override
        public Date deserialize(JsonParser parser, DeserializationContext context) throws IOException {

            String unixTimestamp = parser.getText().trim();
            return new Date(TimeUnit.SECONDS.toMillis(Long.parseLong(unixTimestamp)));
        }
    }

    @JsonProperty("n")
    public long sensorId;

    @JsonDeserialize(using = UnixTimestampDeserializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="UTC")
    @JsonProperty("t")
    public Date timestamp;

    @JsonProperty("v")
    public double rawValue;

    public String location = "UNKNOWN";
    public String type = "UNKNOWN";
    public String unit = "UNKNOWN";
    public double conValue = -1D;

    public SensorData() { }

    public SensorData(long sensorId, Date timestamp, double rawValue) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.rawValue = rawValue;
    }

    public Tuple7<Long, Date, String, String, String, Double, Double> toTuple() {
        return new Tuple7<>(sensorId, timestamp, type, location, unit, rawValue, conValue);
    }

    public String toString() {
        return
            "{ " +
            "\"n\":  " + sensorId + ", " +
            "\"t\": " + timestamp.getTime() + ", " +
            "\"v\": " + rawValue + ", " +
            "\"g\": \"" + location + "\", " +
            "\"ty\": \"" + type + "\", " +
            "\"u\": \"" + unit + "\", " +
            "\"c\": " + conValue +
            " }";
    }

    public long getSensorId() {
        return sensorId;
    }
}
