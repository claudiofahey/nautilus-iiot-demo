# MQTT to Pravega Bridge

This sample application reads events from MQTT and writes them to a Pravega stream.

# Usage

- Install Mosquitto MQTT broker and clients.
  ```
  sudo apt-get install mosquitto mosquitto-clients
  ```

- Start Mosquitto broker.
  ```
  mosquitto
  ```

- In IntelliJ, run the class com.dell.mqtt.pravega.ApplicationMain with the following parameters:
  ```
  mqtt-pravega-bridge/src/main/dist/conf
  ```

- Publish a sample MQTT message.
  ```
  mosquitto_pub -t center/0001 -m "12,34,56.78"
  ```

- You should see the following application output:
  ```
  [MQTT Call: CanDataReader] com.dell.mqtt.pravega.MqttListener: Writing Data Packet: CarID: 0001 Timestamp: 1551671403118 Payload: [B@2813d92f annotation: null
  ```
