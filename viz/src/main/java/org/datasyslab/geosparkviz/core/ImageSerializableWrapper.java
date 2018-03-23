/*
 * FILE: ImageSerializableWrapper
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geosparkviz.core;

import org.apache.log4j.Logger;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Class ImageSerializableWrapper.
 */
public class ImageSerializableWrapper
        implements Serializable
{
    final static Logger log = Logger.getLogger(ImageSerializableWrapper.class);
    /**
     * The image.
     */
    protected transient BufferedImage image;

    /**
     * Instantiates a new image serializable wrapper.
     *
     * @param image the image
     */
    public ImageSerializableWrapper(BufferedImage image)
    {
        this.image = image;
    }

    /**
     * Write object.
     *
     * @param out the out
     * @throws IOException Signals that an I/O exception has occurred.
     */
    // Serialization method.
    private void writeObject(ObjectOutputStream out)
            throws IOException
    {
        log.debug("Serializing ImageWrapper");
        out.defaultWriteObject();
        ImageIO.write(image, "png", out);
    }

    /**
     * Read object.
     *
     * @param in the in
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     */
    // Deserialization method.
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException
    {
        log.debug("De-serializing ImageWrapper");
        in.defaultReadObject();
        image = ImageIO.read(in);
        if (image == null) {
            System.out.println("I got nothing from the stream!");
        }
    }

    /**
     * Gets the image.
     *
     * @return the image
     */
    public BufferedImage getImage()
    {
        return this.image;
    }

    @Override
    public String toString()
    {
        return "Image(" +
                "width=" + image.getWidth() +
                "height=" + image.getHeight() +
                ')';
    }
}