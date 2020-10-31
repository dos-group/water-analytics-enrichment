package de.tu_berlin.cit.watergridsense_pipelines.utils;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Describes an alert for which a sensor value was outside the expected bounds.
 */
public class DataAlert {
    private static class UnixTimestampDeserializer extends JsonDeserializer<Date> {

        @Override
        public Date deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            String unixTimestamp = parser.getText().trim();
            return new Date(TimeUnit.SECONDS.toMillis(Long.parseLong(unixTimestamp)));
        }
    }

    @JsonProperty("n")
    public long sensorId;
    @JsonDeserialize(using = DataAlert.UnixTimestampDeserializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="UTC")
    @JsonProperty("t")
    public Date timestamp;
    @JsonProperty("v")
    public double value;
    @JsonProperty("min")
    public double lowerThreshold;
    @JsonProperty("max")
    public double upperThreshold;

    public DataAlert(long sensorId, Date timestamp, double value, double lowerThreshold, double upperThreshold) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.value = value;
        this.lowerThreshold = lowerThreshold;
        this.upperThreshold = upperThreshold;
    }

    /**
     * Will return a positive float that indicates alert severity on the
     * basis of distance from the allowed confidence interval.
     * @return The factor w.r.t. the confidence interval size that the
     *         value is distant from the closest interval boundary.
     */
    public double getSeverity() {
        double difference = upperThreshold - lowerThreshold;

        double dist;
        if (value < lowerThreshold) {
            dist = lowerThreshold - value;
        } else {
            dist = value - upperThreshold;
        }

        return dist / difference;
    }

    public String toString() {
        return
            "{ " +
            "\"n\":  " + sensorId + ", " +
            "\"t\": " + timestamp.getTime() + ", " +
            "\"v\": " + value + ", " +
            "\"min\": \"" + lowerThreshold + "\", " +
            "\"max\": \"" + upperThreshold + "\", " +
            " }";
    }
}
