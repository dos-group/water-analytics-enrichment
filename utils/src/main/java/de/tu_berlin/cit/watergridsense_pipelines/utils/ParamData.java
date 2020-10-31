package de.tu_berlin.cit.watergridsense_pipelines.utils;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ParamData {

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
    @JsonProperty("p")
    public String parameter;
    @JsonProperty("v")
    public String value;

    public ParamData() { }

    public ParamData(long sensorId, Date timestamp, String parameter, String value) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.parameter = parameter;
        this.value = value;
    }
    public Tuple4<Long, Date, String, String> toTuple() {
        return new Tuple4<>(sensorId, timestamp, parameter, value);
    }

    public String toString() {
        return
            "{ " +
            "\"n\": " + sensorId + ", " +
            "\"t\": " + timestamp.getTime() + ", " +
            "\"p\": \"" + parameter + "\", " +
            "\"v\": " + value +
            " }";
    }

    public long getSensorId() {
        return sensorId;
    }
}
