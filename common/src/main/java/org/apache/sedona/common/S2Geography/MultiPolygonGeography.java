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
package org.apache.sedona.common.S2Geography;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.S2Polygon;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class MultiPolygonGeography extends GeographyCollection {
  private static final Logger logger = Logger.getLogger(MultiPolygonGeography.class.getName());
  /**
   * Wrap each raw S2Polygon in a PolygonGeography, then hand it off to GeographyCollection to do
   * the rest (including serialization).
   */
  public MultiPolygonGeography(GeographyKind kind, List<S2Polygon> polygons) {
    super(kind, polygons);
  }

  public MultiPolygonGeography() {
    super(Collections.emptyList());
  }

  public List<Geography> getFeatures() {
    return features;
  }

  @Override
  public int dimension() {
    // every child PolygonGeography
    return 2;
  }

  @Override
  public void encode(UnsafeOutput out, EncodeOptions opts) throws IOException {
    out.writeInt(features.size());
    for (Geography feature : features) {
      ((PolygonGeography) feature).polygon.encode(out);
    }
    out.flush();
  }

  /** This is what decodeTagged() actually calls */
  public static MultiPolygonGeography decode(Input in, EncodeTag tag) throws IOException {
    // cast to UnsafeInput—will work if you always pass a Kryo-backed stream
    if (!(in instanceof UnsafeInput)) {
      throw new IllegalArgumentException("Expected UnsafeInput");
    }
    return decode((UnsafeInput) in, tag);
  }

  /** Decodes a GeographyCollection from a tagged input stream. */
  public static MultiPolygonGeography decode(UnsafeInput in, EncodeTag tag)
      throws IOException, EOFException {
    // Handle EMPTY flag
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty Multipolygon.");
      return new MultiPolygonGeography();
    }

    // Skip any covering data
    tag.skipCovering(in);

    MultiPolygonGeography geo;
    try {
      int count = in.readInt();
      if (count < 0) {
        throw new IOException("MultiPolygon.decodeTagged error: negative polygon count: " + count);
      }

      List<S2Polygon> polygons = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        polygons.add(S2Polygon.decode(in));
      }

      geo = new MultiPolygonGeography(GeographyKind.MULTIPOLYGON, polygons);
      geo.countShapes();

    } catch (EOFException e) {
      throw new IOException(
          "MultiPolygon.decodeTagged error: insufficient data to decode all parts of the geography.",
          e);
    }
    return geo;
  }
}
