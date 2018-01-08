/**
 * FILE: ImageSerializableWrapper.java
 * PATH: org.datasyslab.geosparkviz.core.ImageSerializableWrapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geosparkviz.core;

import org.apache.log4j.Logger;

import java.io.*;

import javax.imageio.ImageIO;

import java.awt.image.*;

// TODO: Auto-generated Javadoc
/**
 * The Class ImageSerializableWrapper.
 */
public class ImageSerializableWrapper implements Serializable {
  final static Logger log = Logger.getLogger(ImageSerializableWrapper.class);
  /** The image. */
  protected transient BufferedImage image;
  
  /**
   * Instantiates a new image serializable wrapper.
   *
   * @param image the image
   */
  public ImageSerializableWrapper(BufferedImage image) {
    this.image = image;
  }

  /**
   * Write object.
   *
   * @param out the out
   * @throws IOException Signals that an I/O exception has occurred.
   */
  // Serialization method.
  private void writeObject(ObjectOutputStream out) throws IOException {
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
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    log.debug("De-serializing ImageWrapper");
    in.defaultReadObject();
    image = ImageIO.read(in);
    if(image==null)
    {
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
}