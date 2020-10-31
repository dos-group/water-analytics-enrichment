package de.tu_berlin.cit.watergridsense_pipelines.utils;

import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Parameter cache contains the current and previous value of a given parameter
 */
public class ParamCache {

    public Tuple2<Date, String> current;
    public Tuple2<Date, String> previous;

    public String toString() {
        String returnString = "";
        if (this.current != null) {
            returnString = returnString +
            "\nCurrent: " +
            new Date(this.current.f0.getTime()) +
            ", " +
            this.current.f1;
        }
        else {
            returnString = returnString +
            "Current: null";
        }
        if (this.previous != null) {
            returnString = returnString + 
            "\nPrevious: " +
            new Date(this.previous.f0.getTime()) +
            ", " +
            this.previous.f1;
        }
        else {
            returnString = returnString + 
                "\nPrevious: null";
        }
        return returnString;
    }
}