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
package org.apache.sedona.common.utils;

import java.awt.Image;
import java.awt.Rectangle;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.Arrays;
import java.util.Vector;

/**
 * A {@link RenderedImage} that delegates everything to a source image but hides a single property.
 *
 * <p>Image properties cannot be removed in place: {@link javax.media.jai.RenderedImageAdapter}
 * declares {@code getProperty} final and answers from the source, so {@code removeProperty} cannot
 * mask an inherited value. Wrapping is also the only way to drop a property without touching pixels
 * — copying the raster would materialize a lazily decoded image and lose a non-zero image origin.
 */
public class PropertyMaskedRenderedImage implements RenderedImage {
  private final RenderedImage source;
  private final String maskedProperty;

  private PropertyMaskedRenderedImage(RenderedImage source, String maskedProperty) {
    this.source = source;
    this.maskedProperty = maskedProperty;
  }

  /**
   * Wrap {@code source} so that {@code propertyName} reads back as {@link Image#UndefinedProperty}.
   * Returns the source unchanged when it does not carry the property.
   */
  public static RenderedImage mask(RenderedImage source, String propertyName) {
    if (source.getProperty(propertyName) == Image.UndefinedProperty) {
      return source;
    }
    return new PropertyMaskedRenderedImage(source, propertyName);
  }

  @Override
  public Object getProperty(String name) {
    if (maskedProperty.equalsIgnoreCase(name)) {
      return Image.UndefinedProperty;
    }
    return source.getProperty(name);
  }

  @Override
  public String[] getPropertyNames() {
    String[] names = source.getPropertyNames();
    if (names == null) {
      return null;
    }
    String[] retained =
        Arrays.stream(names)
            .filter(name -> !maskedProperty.equalsIgnoreCase(name))
            .toArray(String[]::new);
    // RenderedImage uses null, not an empty array, to mean "no properties".
    return retained.length == 0 ? null : retained;
  }

  @Override
  public Vector<RenderedImage> getSources() {
    return source.getSources();
  }

  @Override
  public ColorModel getColorModel() {
    return source.getColorModel();
  }

  @Override
  public SampleModel getSampleModel() {
    return source.getSampleModel();
  }

  @Override
  public int getWidth() {
    return source.getWidth();
  }

  @Override
  public int getHeight() {
    return source.getHeight();
  }

  @Override
  public int getMinX() {
    return source.getMinX();
  }

  @Override
  public int getMinY() {
    return source.getMinY();
  }

  @Override
  public int getNumXTiles() {
    return source.getNumXTiles();
  }

  @Override
  public int getNumYTiles() {
    return source.getNumYTiles();
  }

  @Override
  public int getMinTileX() {
    return source.getMinTileX();
  }

  @Override
  public int getMinTileY() {
    return source.getMinTileY();
  }

  @Override
  public int getTileWidth() {
    return source.getTileWidth();
  }

  @Override
  public int getTileHeight() {
    return source.getTileHeight();
  }

  @Override
  public int getTileGridXOffset() {
    return source.getTileGridXOffset();
  }

  @Override
  public int getTileGridYOffset() {
    return source.getTileGridYOffset();
  }

  @Override
  public Raster getTile(int tileX, int tileY) {
    return source.getTile(tileX, tileY);
  }

  @Override
  public Raster getData() {
    return source.getData();
  }

  @Override
  public Raster getData(Rectangle rect) {
    return source.getData(rect);
  }

  @Override
  public WritableRaster copyData(WritableRaster raster) {
    return source.copyData(raster);
  }
}
