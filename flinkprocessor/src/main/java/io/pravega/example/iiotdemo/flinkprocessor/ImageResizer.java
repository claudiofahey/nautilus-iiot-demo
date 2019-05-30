package io.pravega.example.iiotdemo.flinkprocessor;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * Resizes images to a fixed size.
 */
public class ImageResizer {
    private final int outputWidth;
    private final int outputHeight;

    public ImageResizer(int outputWidth, int outputHeight) {
        this.outputWidth = outputWidth;
        this.outputHeight = outputHeight;
    }

    /**
     *
     * @param image Image file bytes
     * @return Image file bytes
     */
    public byte[] resize(byte[] image) {
        try {
            ByteArrayInputStream inStream = new ByteArrayInputStream(image);
            BufferedImage inImage = ImageIO.read(inStream);
            Image scaledImage = inImage.getScaledInstance(outputWidth, outputHeight, Image.SCALE_SMOOTH);
            BufferedImage outImage = new BufferedImage(outputWidth, outputHeight, inImage.getType());
            Graphics2D g2d = outImage.createGraphics();
            g2d.drawImage(scaledImage, 0, 0, null);
            g2d.dispose();
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            ImageIO.write(outImage, "png", outStream);
            return outStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
