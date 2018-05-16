package io.pravega.example.iiotdemo.sparkprocessor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RawData implements Serializable {
    public long timestamp;
    public String event_type;
    public String device_id;
    public double temp_celsius;
    public double vibration1;
    public double vibration2;

    @Override
    public String toString() {
        return "RawData{" +
                "timestamp=" + timestamp +
                ", event_type='" + event_type + '\'' +
                ", device_id='" + device_id + '\'' +
                ", temp_celsius=" + temp_celsius +
                ", vibration1=" + vibration1 +
                ", vibration2=" + vibration2 +
                '}';
    }
}
