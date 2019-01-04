package com.dell.mqtt.pravega;

import java.io.Serializable;

/**
 * Wrapper class that holds raw data and its corresponding annotation info
 */
public class DataPacket implements Serializable {

    private String carId;

    private long timestamp;

    private byte[] payload;

    private byte[] annotation;

    public String getCarId() {
        return carId;
    }

    public void setCarId(String carId) {
        this.carId = carId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public byte[] getAnnotation() {
        return annotation;
    }

    public void setAnnotation(byte[] annotation) {
        this.annotation = annotation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String toString() {

        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("CarID: ").append(carId).append(" ");
        stringBuffer.append("Timestamp: ").append(timestamp).append(" ");
        stringBuffer.append("Payload: ").append(payload).append(" ");
        stringBuffer.append("annotation: ").append(annotation).append(" ");
        return stringBuffer.toString();
    }
}
