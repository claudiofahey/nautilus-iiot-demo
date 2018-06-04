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
        TempData tempData = new TempData(DateTime.now(), "1234", 30.0);
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
        TempData tempData1 = new TempData(DateTime.now(), "1234", 30.0);
        log.info("tempData1={}", tempData1);
        TopRecord topRecord1 = TopRecord.newBuilder().setTempData(tempData1).build();
        log.info("topRecord1={}", topRecord1);

        // Create vibration data record.
        VibrationData vibrationData1 = new VibrationData(DateTime.now(), "1234", 1.0, 2.0);
        log.info("vibrationData1={}", vibrationData1);
        TopRecord topRecord2 = TopRecord.newBuilder().setVibrationData(vibrationData1).build();
        log.info("topRecord2={}", topRecord2);

        // Create invalid data record.
        TopRecord topRecord3 = TopRecord.newBuilder()
                .setTempData(tempData1)
                .setVibrationData(vibrationData1)
                .build();
        log.info("topRecord3={}", topRecord3);

        // Create pressure data record.
        PressureData pressureData1 = new PressureData(DateTime.now(), "1234", 1000.0);
        log.info("pressureData1={}", pressureData1);
        TopRecord topRecord4 = TopRecord.newBuilder().setPressureData(pressureData1).build();
        log.info("topRecord4={}", topRecord4);

        // Serialize records to file.
        String fileName = "/tmp/test1.avro";
        DatumWriter<TopRecord> datumWriter = new SpecificDatumWriter<TopRecord>(TopRecord.class);
        DataFileWriter<TopRecord> dataFileWriter = new DataFileWriter<TopRecord>(datumWriter);
        dataFileWriter.create(TopRecord.getClassSchema(), new File(fileName));
        dataFileWriter.append(topRecord1);
        dataFileWriter.append(topRecord2);
        dataFileWriter.append(topRecord3);
        dataFileWriter.append(topRecord4);
        dataFileWriter.close();

        // Deserialize records from file and handle them.
        DatumReader<TopRecord> datumReader = new SpecificDatumReader<TopRecord>(TopRecord.class);
        DataFileReader<TopRecord> dataFileReader = new DataFileReader<TopRecord>(new File(fileName), datumReader);
        TopRecord topRecord = null;
        while (dataFileReader.hasNext()) {
            topRecord = dataFileReader.next(topRecord);
            log.info("topRecord={}", topRecord);
            if (topRecord.getTempData() != null && topRecord.getVibrationData() != null) {
                log.warn("Skipping invalid record that has both TempData and VibrationData: {}", topRecord);
                continue;
            }
            if (topRecord.getTempData() != null) {
                TempData tempData = topRecord.getTempData();
                // Handle TempData.
                log.info("tempData={}", tempData);
            }
            if (topRecord.getVibrationData() != null) {
                VibrationData vibrationData = topRecord.getVibrationData();
                // Handle VibrationData.
                log.info("vibrationData={}", vibrationData);
            }
        }
    }

    @Test
    public void Test3() throws Exception {
        String fileName = "/tmp/test1.avro";
        DatumReader<TopRecord> datumReader = new SpecificDatumReader<TopRecord>(TopRecord.class);
        DataFileReader<TopRecord> dataFileReader = new DataFileReader<TopRecord>(new File(fileName), datumReader);
        TopRecord topRecord = null;
        while (dataFileReader.hasNext()) {
            topRecord = dataFileReader.next(topRecord);
            log.info("topRecord={}", topRecord);
            if (topRecord.getTempData() != null && topRecord.getVibrationData() != null) {
                log.warn("Skipping invalid record that has both TempData and VibrationData: {}", topRecord);
                continue;
            }
            if (topRecord.getTempData() != null) {
                TempData tempData = topRecord.getTempData();
                // Handle TempData.
                log.info("tempData={}", tempData);
            }
            if (topRecord.getVibrationData() != null) {
                VibrationData vibrationData = topRecord.getVibrationData();
                // Handle VibrationData.
                log.info("vibrationData={}", vibrationData);
            }
            if (topRecord.getPressureData() != null) {
                PressureData pressureData = topRecord.getPressureData();
                // Handle PressureData.
                log.info("pressureData={}", pressureData);
            }
        }
    }

}
