/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.viz.core;

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