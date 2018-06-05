package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class AvroTest1 {
    private static Logger log = LoggerFactory.getLogger(AvroTest1.class);

    @Test
    public void Test1() throws Exception {
        TempData tempData = new TempData(30.0);
        log.info("tempData={}", tempData);

        DatumWriter<TempData> datumWriter = new SpecificDatumWriter<TempData>(TempData.class);
        DataFileWriter<TempData> dataFileWriter = new DataFileWriter<TempData>(datumWriter);
        dataFileWriter.create(tempData.getSchema(), new File("/tmp/test1.avro"));
        dataFileWriter.append(tempData);
        dataFileWriter.close();
    }

    // This demonstrates a form of union in Avro.
    // We actually don't want to use an Avro union type because that does not allow record types
    // to be added and removed as schemas evolve.
    // Instead, each record type is just a different field in a record.
    // This Avro schema would allow a record with multiple type of records (e.g. TempData and VibrationData).
    // This should be handled by the reader as required.
    @Test
    public void Test2() throws Exception {
        // Create temperature data record.
        TempData tempData1 = new TempData(30.0);
        log.info("tempData1={}", tempData1);
        GenericData genericData1 = GenericData.newBuilder()
                .setTimestamp(DateTime.now())
                .setDeviceId("1234")
                .setTempData(tempData1)
                .build();
        log.info("genericData1={}", genericData1);

        // Create vibration data record.
        VibrationData vibrationData1 = new VibrationData( 1.0, 2.0);
        log.info("vibrationData1={}", vibrationData1);
        GenericData genericData2 = GenericData.newBuilder()
                .setTimestamp(DateTime.now())
                .setDeviceId("1234")
                .setVibrationData(vibrationData1)
                .build();
        log.info("genericData2={}", genericData2);

        // Create invalid data record.
        GenericData genericData3 = GenericData.newBuilder()
                .setTimestamp(DateTime.now())
                .setDeviceId("1234")
                .setTempData(tempData1)
                .setVibrationData(vibrationData1)
                .build();
        log.info("genericData3={}", genericData3);

        // Create pressure data record.
        PressureData pressureData1 = new PressureData(1000.0);
        log.info("pressureData1={}", pressureData1);
        GenericData genericData4 = GenericData.newBuilder()
                .setTimestamp(DateTime.now())
                .setDeviceId("1234")
                .setPressureData(pressureData1)
                .build();
        log.info("genericData4={}", genericData4);

        // Serialize records to file.
        String fileName = "/tmp/test1.avro";
        DatumWriter<GenericData> datumWriter = new SpecificDatumWriter<GenericData>(GenericData.class);
        DataFileWriter<GenericData> dataFileWriter = new DataFileWriter<GenericData>(datumWriter);
        dataFileWriter.create(GenericData.getClassSchema(), new File(fileName));
        dataFileWriter.append(genericData1);
        dataFileWriter.append(genericData2);
        dataFileWriter.append(genericData3);
        dataFileWriter.append(genericData4);
        dataFileWriter.close();

        // Deserialize records from file and handle them.
        DatumReader<GenericData> datumReader = new SpecificDatumReader<GenericData>(GenericData.class);
        DataFileReader<GenericData> dataFileReader = new DataFileReader<GenericData>(new File(fileName), datumReader);
        GenericData genericData = null;
        while (dataFileReader.hasNext()) {
            genericData = dataFileReader.next(genericData);
            log.info("genericData={}", genericData);
            if (genericData.getTempData() != null && genericData.getVibrationData() != null) {
                log.warn("Skipping invalid record that has both TempData and VibrationData: {}", genericData);
                continue;
            }
            if (genericData.getTempData() != null) {
                TempData tempData = genericData.getTempData();
                // Handle TempData.
                log.info("tempData={}", tempData);
            }
            if (genericData.getVibrationData() != null) {
                VibrationData vibrationData = genericData.getVibrationData();
                // Handle VibrationData.
                log.info("vibrationData={}", vibrationData);
            }
        }
    }

    @Test
    public void Test3() throws Exception {
        String fileName = "/tmp/test1.avro";
        DatumReader<GenericData> datumReader = new SpecificDatumReader<GenericData>(GenericData.class);
        DataFileReader<GenericData> dataFileReader = new DataFileReader<GenericData>(new File(fileName), datumReader);
        GenericData genericData = null;
        while (dataFileReader.hasNext()) {
            genericData = dataFileReader.next(genericData);
            log.info("genericData={}", genericData);
            if (genericData.getTempData() != null && genericData.getVibrationData() != null) {
                log.warn("Skipping invalid record that has both TempData and VibrationData: {}", genericData);
                continue;
            }
            if (genericData.getTempData() != null) {
                TempData tempData = genericData.getTempData();
                // Handle TempData.
                log.info("tempData={}", tempData);
            }
            if (genericData.getVibrationData() != null) {
                VibrationData vibrationData = genericData.getVibrationData();
                // Handle VibrationData.
                log.info("vibrationData={}", vibrationData);
            }
            if (genericData.getPressureData() != null) {
                PressureData pressureData = genericData.getPressureData();
                // Handle PressureData.
                log.info("pressureData={}", pressureData);
            }
        }
    }

}
