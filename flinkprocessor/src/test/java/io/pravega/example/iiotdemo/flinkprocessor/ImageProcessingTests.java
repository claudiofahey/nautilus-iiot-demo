package io.pravega.example.iiotdemo.flinkprocessor;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class ImageProcessingTests {
    private static Logger log = LoggerFactory.getLogger(ImageProcessingTests.class);

    @Test
    @Ignore
    public void Test1() throws Exception {

        byte[] inBytes = Files.readAllBytes((new File("/tmp/camera0-0.png")).toPath());
        ByteArrayInputStream inStream = new ByteArrayInputStream(inBytes);
        BufferedImage inImage = ImageIO.read(inStream);
        log.info("inImage={}", inImage);

        BufferedImage outImage = new BufferedImage(inImage.getWidth()*2, inImage.getHeight()*2, inImage.getType());
        outImage.getRaster().setRect(0, 0, inImage.getData());
        log.info("outImage={}", outImage);

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ImageIO.write(outImage, "png", outStream);

//        ImageWriter writer = ImageIO.getImageWritersByFormatName("png").next();
//        ImageWriteParam writeParam = writer.getDefaultWriteParam();
//        writeParam.setCompressionMode(ImageWriteParam.MODE_DISABLED); // Not supported exception
//        writer.setOutput(outStream);
//        writer.write(null, new IIOImage(outImage, null, null), writeParam);

        byte[] outBytes = outStream.toByteArray();
        Files.write((new File("/tmp/out2.png")).toPath(), outBytes);
    }

    @Test
    @Ignore
    public void Test2() throws Exception {
        byte[] inBytes = Files.readAllBytes((new File("/tmp/camera0-0.png")).toPath());
        Map<Integer, byte[]> images = new HashMap<>();
        images.put(0, inBytes);
        images.put(3, inBytes);
        ImageGridBuilder builder = new ImageGridBuilder(19, 19, 4);
        builder.addImage(1, inBytes);
        builder.addImages(images);
        byte[] outBytes = builder.getOutputImageBytes("png");
        Files.write((new File("/tmp/out3.png")).toPath(), outBytes);
    }

    @Test
    public void Test3() throws Exception {
        byte[] inBytes = Files.readAllBytes((new File("/tmp/camera0-0.png")).toPath());
        ImageResizer resizer = new ImageResizer(100, 100);
        byte[] outBytes = resizer.resize(inBytes);
        Files.write((new File("/tmp/out4.png")).toPath(), outBytes);
    }
}
