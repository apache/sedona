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
package org.apache.sedona.common.raster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.sun.media.jai.rmi.ColorModelState;
import com.sun.media.jai.util.ImageUtil;
import it.geosolutions.jaiext.range.NoDataContainer;
import java.awt.Image;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.RasterAccessor;
import javax.media.jai.RasterFormatTag;
import javax.media.jai.RemoteImage;
import javax.media.jai.TileCache;
import javax.media.jai.remote.SerializableState;
import javax.media.jai.remote.SerializerFactory;
import org.apache.sedona.common.raster.serde.AWTRasterSerializer;
import org.apache.sedona.common.raster.serde.KryoUtil;
import org.apache.sedona.common.utils.RasterUtils;

/**
 * This class is mostly copied from {@link javax.media.jai.remote.SerializableRenderedImage}. We've
 * removed the shallow copy support and fixed a bug of SerializableRenderedImage: When a deep-copied
 * serializable rendered image object is being disposed, it tries to connect to the remote server.
 * However, there is no remote server in deep-copy mode, so the dispose() method throws a
 * java.net.SocketException.
 */
public final class DeepCopiedRenderedImage
    implements RenderedImage, Serializable, KryoSerializable {
  private transient RenderedImage source;
  private int minX;
  private int minY;
  private int width;
  private int height;
  private int minTileX;
  private int minTileY;
  private int numXTiles;
  private int numYTiles;
  private int tileWidth;
  private int tileHeight;
  private int tileGridXOffset;
  private int tileGridYOffset;
  private transient SampleModel sampleModel;
  private transient ColorModel colorModel;
  private transient Vector<RenderedImage> sources;
  private transient Hashtable<String, Object> properties;
  private Rectangle imageBounds;
  private transient Raster imageRaster;

  public DeepCopiedRenderedImage() {
    this.sampleModel = null;
    this.colorModel = null;
    this.sources = null;
    this.properties = null;
  }

  public DeepCopiedRenderedImage(RenderedImage source) {
    this(source, true);
  }

  private DeepCopiedRenderedImage(RenderedImage source, boolean checkDataBuffer) {
    this.sampleModel = null;
    this.colorModel = null;
    this.sources = null;
    this.properties = null;
    if (source == null) {
      throw new IllegalArgumentException("source cannot be null");
    }
    SampleModel sm = source.getSampleModel();
    if (sm != null && SerializerFactory.getSerializer(sm.getClass()) == null) {
      throw new IllegalArgumentException("sample model object is not serializable");
    }
    ColorModel cm = source.getColorModel();
    if (cm != null && SerializerFactory.getSerializer(cm.getClass()) == null) {
      throw new IllegalArgumentException("color model object is not serializable");
    }
    if (checkDataBuffer) {
      Raster ras = source.getTile(source.getMinTileX(), source.getMinTileY());
      if (ras != null) {
        DataBuffer db = ras.getDataBuffer();
        if (db != null && SerializerFactory.getSerializer(db.getClass()) == null) {
          throw new IllegalArgumentException("data buffer object is not serializable");
        }
      }
    }

    this.source = source;
    if (source instanceof RemoteImage) {
      throw new IllegalArgumentException("RemoteImage is not supported");
    }
    this.minX = source.getMinX();
    this.minY = source.getMinY();
    this.width = source.getWidth();
    this.height = source.getHeight();
    this.minTileX = source.getMinTileX();
    this.minTileY = source.getMinTileY();
    this.numXTiles = source.getNumXTiles();
    this.numYTiles = source.getNumYTiles();
    this.tileWidth = source.getTileWidth();
    this.tileHeight = source.getTileHeight();
    this.tileGridXOffset = source.getTileGridXOffset();
    this.tileGridYOffset = source.getTileGridYOffset();
    this.sampleModel = source.getSampleModel();
    this.colorModel = source.getColorModel();
    this.sources = new Vector<>();
    this.sources.add(source);
    this.properties = new Hashtable<>();
    String[] propertyNames = source.getPropertyNames();
    if (propertyNames != null) {
      for (String propertyName : propertyNames) {
        this.properties.put(propertyName, source.getProperty(propertyName));
      }
    }

    this.imageBounds = new Rectangle(this.minX, this.minY, this.width, this.height);
  }

  @Override
  public ColorModel getColorModel() {
    return this.colorModel;
  }

  @Override
  public Raster getData() {
    if (source == null) {
      return this.getData(this.imageBounds);
    } else {
      return this.source.getData();
    }
  }

  @Override
  public Raster getData(Rectangle rect) {
    if (source == null) {
      return this.imageRaster.createChild(
          rect.x, rect.y, rect.width, rect.height, rect.x, rect.y, (int[]) null);
    } else {
      return this.source.getData(rect);
    }
  }

  @Override
  public WritableRaster copyData(WritableRaster dest) {
    if (source == null) {
      Rectangle region;
      if (dest == null) {
        region = this.imageBounds;
        SampleModel destSM =
            this.getSampleModel().createCompatibleSampleModel(region.width, region.height);
        dest = Raster.createWritableRaster(destSM, new Point(region.x, region.y));
      } else {
        region = dest.getBounds().intersection(this.imageBounds);
      }

      if (!region.isEmpty()) {
        int startTileX = PlanarImage.XToTileX(region.x, this.tileGridXOffset, this.tileWidth);
        int startTileY = PlanarImage.YToTileY(region.y, this.tileGridYOffset, this.tileHeight);
        int endTileX =
            PlanarImage.XToTileX(region.x + region.width - 1, this.tileGridXOffset, this.tileWidth);
        int endTileY =
            PlanarImage.YToTileY(
                region.y + region.height - 1, this.tileGridYOffset, this.tileHeight);
        SampleModel[] sampleModels = new SampleModel[] {this.getSampleModel()};
        int tagID = RasterAccessor.findCompatibleTag(sampleModels, dest.getSampleModel());
        RasterFormatTag srcTag = new RasterFormatTag(this.getSampleModel(), tagID);
        RasterFormatTag dstTag = new RasterFormatTag(dest.getSampleModel(), tagID);

        for (int ty = startTileY; ty <= endTileY; ++ty) {
          for (int tx = startTileX; tx <= endTileX; ++tx) {
            Raster tile = this.getTile(tx, ty);
            Rectangle subRegion = region.intersection(tile.getBounds());
            RasterAccessor s = new RasterAccessor(tile, subRegion, srcTag, this.getColorModel());
            RasterAccessor d = new RasterAccessor(dest, subRegion, dstTag, null);
            ImageUtil.copyRaster(s, d);
          }
        }
      }

      return dest;
    } else {
      return this.source.copyData(dest);
    }
  }

  @Override
  public int getHeight() {
    return this.height;
  }

  @Override
  public int getMinTileX() {
    return this.minTileX;
  }

  @Override
  public int getMinTileY() {
    return this.minTileY;
  }

  @Override
  public int getMinX() {
    return this.minX;
  }

  @Override
  public int getMinY() {
    return this.minY;
  }

  @Override
  public int getNumXTiles() {
    return this.numXTiles;
  }

  @Override
  public int getNumYTiles() {
    return this.numYTiles;
  }

  @Override
  public Object getProperty(String name) {
    Object property = this.properties.get(name);
    return property == null ? Image.UndefinedProperty : property;
  }

  @Override
  public String[] getPropertyNames() {
    String[] names = null;
    if (!this.properties.isEmpty()) {
      names = new String[this.properties.size()];
      Enumeration<String> keys = this.properties.keys();
      int index = 0;
      while (keys.hasMoreElements()) {
        names[index++] = keys.nextElement();
      }
    }

    return names;
  }

  @Override
  public SampleModel getSampleModel() {
    return this.sampleModel;
  }

  @Override
  public Vector<RenderedImage> getSources() {
    return this.sources;
  }

  @Override
  public Raster getTile(int tileX, int tileY) {
    if (source == null) {
      TileCache cache = JAI.getDefaultInstance().getTileCache();
      if (cache != null) {
        Raster tile = cache.getTile(this, tileX, tileY);
        if (tile != null) {
          return tile;
        }
      }

      Rectangle imageBounds =
          new Rectangle(this.getMinX(), this.getMinY(), this.getWidth(), this.getHeight());
      Rectangle destRect =
          imageBounds.intersection(
              new Rectangle(
                  this.tileXToX(tileX),
                  this.tileYToY(tileY),
                  this.getTileWidth(),
                  this.getTileHeight()));
      Raster tile = this.getData(destRect);
      if (cache != null) {
        cache.add(this, tileX, tileY, tile);
      }

      return tile;
    } else {
      return this.source.getTile(tileX, tileY);
    }
  }

  private int tileXToX(int tx) {
    return PlanarImage.tileXToX(tx, this.getTileGridXOffset(), this.getTileWidth());
  }

  private int tileYToY(int ty) {
    return PlanarImage.tileYToY(ty, this.getTileGridYOffset(), this.getTileHeight());
  }

  @Override
  public int getTileGridXOffset() {
    return this.tileGridXOffset;
  }

  @Override
  public int getTileGridYOffset() {
    return this.tileGridYOffset;
  }

  @Override
  public int getTileHeight() {
    return this.tileHeight;
  }

  @Override
  public int getTileWidth() {
    return this.tileWidth;
  }

  @Override
  public int getWidth() {
    return this.width;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();

    Hashtable<String, Object> propertyTable = getSerializableProperties();
    out.writeObject(SerializerFactory.getState(this.colorModel, null));
    out.writeObject(propertyTable);
    if (this.source != null) {
      Raster serializedRaster = RasterUtils.getRaster(this.source);
      out.writeObject(SerializerFactory.getState(serializedRaster, null));
    } else {
      out.writeObject(SerializerFactory.getState(imageRaster, null));
    }
  }

  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    this.source = null;
    in.defaultReadObject();

    SerializableState cmState = (SerializableState) in.readObject();
    this.colorModel = (ColorModel) cmState.getObject();
    this.properties = (Hashtable<String, Object>) in.readObject();
    for (String key : this.properties.keySet()) {
      Object value = this.properties.get(key);
      // Restore the value of GC_NODATA property as a NoDataContainer object.
      if (value instanceof SingleValueNoDataContainer) {
        SingleValueNoDataContainer noDataContainer = (SingleValueNoDataContainer) value;
        this.properties.put(key, new NoDataContainer(noDataContainer.singleValue));
      }
    }
    SerializableState rasState = (SerializableState) in.readObject();
    this.imageRaster = (Raster) rasState.getObject();

    // The deserialized rendered image contains only one tile (imageRaster). We need to update
    // the sample model and tile properties to reflect this.
    this.sampleModel = this.imageRaster.getSampleModel();
    this.tileWidth = this.width;
    this.tileHeight = this.height;
    this.numXTiles = 1;
    this.numYTiles = 1;
    this.minTileX = 0;
    this.minTileY = 0;
    this.tileGridXOffset = minX;
    this.tileGridYOffset = minY;
  }

  @SuppressWarnings("unchecked")
  private Hashtable<String, Object> getSerializableProperties() {
    // Prepare serialize properties. non-serializable properties won't be serialized.
    Hashtable<String, Object> propertyTable = this.properties;
    boolean propertiesCloned = false;
    Enumeration<String> keys = propertyTable.keys();
    while (keys.hasMoreElements()) {
      String key = keys.nextElement();
      Object value = this.properties.get(key);
      if (!(value instanceof Serializable)) {
        if (!propertiesCloned) {
          propertyTable = (Hashtable<String, Object>) this.properties.clone();
          propertiesCloned = true;
        }
        // GC_NODATA is a special property used by GeoTools. We need to serialize it.
        if (value instanceof NoDataContainer) {
          NoDataContainer noDataContainer = (NoDataContainer) value;
          propertyTable.put(
              key, new SingleValueNoDataContainer(noDataContainer.getAsSingleValue()));
        } else {
          propertyTable.remove(key);
        }
      }
    }
    return propertyTable;
  }

  public static void registerKryo(Kryo kryo) {
    kryo.register(ColorModelState.class, new JavaSerializer());
  }

  private static final AWTRasterSerializer awtRasterSerializer = new AWTRasterSerializer();

  @Override
  public void write(Kryo kryo, Output output) {
    // write basic properties
    output.writeInt(minX);
    output.writeInt(minY);
    output.writeInt(width);
    output.writeInt(height);

    // write properties
    Hashtable<String, Object> propertyTable = getSerializableProperties();
    KryoUtil.writeObjectWithLength(kryo, output, propertyTable);

    // write color model
    SerializableState colorModelState = SerializerFactory.getState(this.colorModel, null);
    KryoUtil.writeObjectWithLength(kryo, output, colorModelState);

    // write raster
    Raster serializedRaster;
    if (this.source != null) {
      serializedRaster = RasterUtils.getRaster(this.source);
    } else {
      serializedRaster = imageRaster;
    }
    awtRasterSerializer.write(kryo, output, serializedRaster);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void read(Kryo kryo, Input input) {
    // read basic properties
    minX = input.readInt();
    minY = input.readInt();
    width = input.readInt();
    height = input.readInt();
    imageBounds = new Rectangle(minX, minY, width, height);

    // read properties
    input.readInt(); // skip the length of the property table
    properties = kryo.readObject(input, Hashtable.class);
    for (String key : this.properties.keySet()) {
      Object value = this.properties.get(key);
      // Restore the value of GC_NODATA property as a NoDataContainer object.
      if (value instanceof SingleValueNoDataContainer) {
        SingleValueNoDataContainer noDataContainer = (SingleValueNoDataContainer) value;
        this.properties.put(key, new NoDataContainer(noDataContainer.singleValue));
      }
    }

    // read color model
    input.readInt(); // skip the length of the color model state
    ColorModelState cmState = kryo.readObject(input, ColorModelState.class);
    this.colorModel = (ColorModel) cmState.getObject();

    // read raster
    this.imageRaster = awtRasterSerializer.read(kryo, input, Raster.class);

    // The deserialized rendered image contains only one tile (imageRaster). We need to update
    // the sample model and tile properties to reflect this.
    this.sampleModel = this.imageRaster.getSampleModel();
    this.tileWidth = this.width;
    this.tileHeight = this.height;
    this.numXTiles = 1;
    this.numYTiles = 1;
    this.minTileX = 0;
    this.minTileY = 0;
    this.tileGridXOffset = minX;
    this.tileGridYOffset = minY;
  }

  /**
   * This class is for serializing NoDataContainer objects. It is mainly used to serialize the value
   * of GC_NODATA property. We only considered the case where the NoDataContainer object contains
   * only one value. However, it will cover most of the real world cases.
   */
  private static class SingleValueNoDataContainer implements Serializable {
    private final double singleValue;

    SingleValueNoDataContainer(double value) {
      singleValue = value;
    }
  }
}
