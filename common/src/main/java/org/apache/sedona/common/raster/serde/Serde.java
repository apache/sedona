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
package org.apache.sedona.common.raster.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import javax.media.jai.RenderedImageAdapter;
import org.apache.sedona.common.raster.DeepCopiedRenderedImage;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.opengis.referencing.operation.MathTransform;

public class Serde {
  private Serde() {}

  /** URIs are not serializable. We need to provide a custom serializer */
  private static class URISerializer extends Serializer<java.net.URI> {
    public URISerializer() {
      setImmutable(true);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final URI uri) {
      KryoUtil.writeUTF8String(output, uri.toString());
    }

    @Override
    public URI read(final Kryo kryo, final Input input, final Class<URI> uriClass) {
      return URI.create(KryoUtil.readUTF8String(input));
    }
  }

  private static final ThreadLocal<Kryo> kryos =
      ThreadLocal.withInitial(
          () -> {
            Kryo kryo = new Kryo();
            kryo.setInstantiatorStrategy(
                new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            kryo.register(AffineTransform2D.class, new AffineTransform2DSerializer());
            kryo.register(GridSampleDimension.class, new GridSampleDimensionSerializer());
            kryo.register(URI.class, new URISerializer());
            DeepCopiedRenderedImage.registerKryo(kryo);
            try {
              kryo.register(
                  Class.forName("org.geotools.coverage.grid.RenderedSampleDimension"),
                  new GridSampleDimensionSerializer());
            } catch (ClassNotFoundException e) {
              throw new RuntimeException(
                  "Cannot register kryo serializer for class RenderedSampleDimension", e);
            }
            kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
            return kryo;
          });

  private static class SerializableState implements Serializable, KryoSerializable {
    public CharSequence name;

    // The following three components are used to construct a GridGeometry2D object.
    // We serialize CRS separately because the default serializer is pretty slow, we use a
    // cached serializer to speed up the serialization and reuse CRS on deserialization.
    public GridEnvelope2D gridEnvelope2D;
    public MathTransform gridToCRS;
    public byte[] serializedCRS;

    public GridSampleDimension[] bands;
    public DeepCopiedRenderedImage image;

    public GridCoverage2D restore() {
      GridGeometry2D gridGeometry =
          new GridGeometry2D(gridEnvelope2D, gridToCRS, CRSSerializer.deserialize(serializedCRS));
      return new GridCoverageFactory().create(name, image, gridGeometry, bands, null, null);
    }

    private static final GridEnvelopeSerializer gridEnvelopeSerializer =
        new GridEnvelopeSerializer();
    private static final AffineTransform2DSerializer affineTransform2DSerializer =
        new AffineTransform2DSerializer();
    private static final GridSampleDimensionSerializer gridSampleDimensionSerializer =
        new GridSampleDimensionSerializer();

    @Override
    public void write(Kryo kryo, Output output) {
      KryoUtil.writeUTF8String(output, name.toString());
      gridEnvelopeSerializer.write(kryo, output, gridEnvelope2D);
      if (!(gridToCRS instanceof AffineTransform2D)) {
        throw new UnsupportedOperationException("Only AffineTransform2D is supported");
      }
      affineTransform2DSerializer.write(kryo, output, (AffineTransform2D) gridToCRS);
      output.writeInt(serializedCRS.length);
      output.writeBytes(serializedCRS);
      output.writeInt(bands.length);
      for (GridSampleDimension band : bands) {
        gridSampleDimensionSerializer.write(kryo, output, band);
      }
      image.write(kryo, output);
    }

    @Override
    public void read(Kryo kryo, Input input) {
      name = KryoUtil.readUTF8String(input);
      gridEnvelope2D = gridEnvelopeSerializer.read(kryo, input, GridEnvelope2D.class);
      gridToCRS = affineTransform2DSerializer.read(kryo, input, AffineTransform2D.class);
      int serializedCRSLength = input.readInt();
      serializedCRS = input.readBytes(serializedCRSLength);
      int bandCount = input.readInt();
      bands = new GridSampleDimension[bandCount];
      for (int i = 0; i < bandCount; i++) {
        bands[i] = gridSampleDimensionSerializer.read(kryo, input, GridSampleDimension.class);
      }
      image = new DeepCopiedRenderedImage();
      image.read(kryo, input);
    }
  }

  // A byte reserved for supporting rasters with other storage schemes
  private static final int IN_DB = 0;

  public static byte[] serialize(GridCoverage2D raster) throws IOException {
    Kryo kryo = kryos.get();
    // GridCoverage2D created by GridCoverage2DReaders contain references that are not serializable.
    // Wrap the RenderedImage in DeepCopiedRenderedImage to make it serializable.
    DeepCopiedRenderedImage deepCopiedRenderedImage = null;
    RenderedImage renderedImage = raster.getRenderedImage();
    while (renderedImage instanceof RenderedImageAdapter) {
      renderedImage = ((RenderedImageAdapter) renderedImage).getWrappedImage();
    }
    if (renderedImage instanceof DeepCopiedRenderedImage) {
      deepCopiedRenderedImage = (DeepCopiedRenderedImage) renderedImage;
    } else {
      deepCopiedRenderedImage = new DeepCopiedRenderedImage(renderedImage);
    }

    SerializableState state = new SerializableState();
    GridGeometry2D gridGeometry = raster.getGridGeometry();
    state.name = raster.getName();
    state.gridEnvelope2D = gridGeometry.getGridRange2D();
    state.gridToCRS = gridGeometry.getGridToCRS2D();
    state.serializedCRS = CRSSerializer.serialize(gridGeometry.getCoordinateReferenceSystem());
    state.bands = raster.getSampleDimensions();
    state.image = deepCopiedRenderedImage;
    try (UnsafeOutput out = new UnsafeOutput(4096, -1)) {
      out.writeByte(IN_DB);
      state.write(kryo, out);
      return out.toBytes();
    }
  }

  public static GridCoverage2D deserialize(byte[] bytes)
      throws IOException, ClassNotFoundException {
    Kryo kryo = kryos.get();
    try (UnsafeInput in = new UnsafeInput(bytes)) {
      int rasterType = in.readByte();
      if (rasterType != IN_DB) {
        throw new IllegalArgumentException("Unsupported raster type: " + rasterType);
      }
      SerializableState state = new SerializableState();
      state.read(kryo, in);
      return state.restore();
    }
  }
}
