package de.tu_berlin.cit.watergridsense_pipelines.utils;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ParamDataTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<ParamData> {

    public ParamDataTSExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(ParamData paramData) {
        return paramData.timestamp.toInstant().toEpochMilli();
    }
}