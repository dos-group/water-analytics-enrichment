package de.tu_berlin.cit.watergridsense_pipelines.utils;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SensorDataTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<SensorData> {

    public SensorDataTSExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(SensorData sensorData) {
        return sensorData.timestamp.toInstant().toEpochMilli();
    }
}