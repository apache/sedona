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

import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.geometry.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/** A Geography wrapping zero or more Geography objects, representing a GEOMETRYCOLLECTION. */
public class GeographyCollection extends Geography {
  private static final Logger logger = Logger.getLogger(GeographyCollection.class.getName());

  public final List<Geography> features;
  public final List<Integer> numShapesList;
  public int totalShapes;

  /** Constructs an empty GeographyCollection. */
  public GeographyCollection() {
    super(GeographyKind.GEOGRAPHY_COLLECTION);
    this.features = new ArrayList<>();
    this.numShapesList = new ArrayList<>();
    this.totalShapes = 0;
  }

  public GeographyCollection(GeographyKind kind, List<S2Polygon> polygons) {
    super(kind); // can be MULTIPOLYGON
    if (kind != GeographyKind.MULTIPOLYGON) {
      throw new IllegalArgumentException(
          "Invalid GeographyKind, only allow Multipolygon as in geographyCollection: " + kind);
    }
    List<S2Polygon> inputPolygons = (polygons == null) ? Collections.emptyList() : polygons;
    // Create the list of features.
    this.features =
        inputPolygons.stream().map(PolygonGeography::new).collect(Collectors.toUnmodifiableList());
    this.numShapesList = new ArrayList<>();
    this.totalShapes = 0;
    countShapes();
  }

  /** Wraps existing Geography features. */
  public GeographyCollection(List<Geography> features) {
    super(GeographyKind.GEOGRAPHY_COLLECTION);
    this.features = new ArrayList<>(features);
    this.numShapesList = new ArrayList<>();
    this.totalShapes = 0;
    countShapes();
  }

  @Override
  public int dimension() {
    // Mixed or empty → return -1; uniform → return 0,1,2
    return computeDimensionFromShapes();
  }

  @Override
  public int numShapes() {
    return totalShapes;
  }

  @Override
  public S2Shape shape(int id) {
    int sum = 0;
    for (int i = 0; i < features.size(); i++) {
      int n = numShapesList.get(i);
      sum += n;
      if (id < sum) {
        // index within this feature
        return features.get(i).shape(id - (sum - n));
      }
    }
    throw new IllegalArgumentException("Shape id out of bounds: " + id);
  }

  @Override
  public S2Region region() {
    Collection<S2Region> regs = new ArrayList<>();
    for (Geography geo : features) {
      regs.add(geo.region());
    }
    return new S2RegionUnion(regs);
  }

  /** Returns an immutable copy of the features list. */
  public List<Geography> getFeatures() {
    return ImmutableList.copyOf(features);
  }

  @Override
  public void encode(UnsafeOutput out, EncodeOptions opts) throws IOException {
    // Top-level collection encodes its size and then each child with tagging
    // Never include coverings for children (only a top-level concept
    EncodeOptions childOptions = new EncodeOptions(opts);
    childOptions.setIncludeCovering(false);
    out.writeInt(features.size());
    for (Geography feature : features) {
      feature.encodeTagged(out, opts);
    }
    out.flush();
  }

  /** Decodes a GeographyCollection from a tagged input stream. */
  public static GeographyCollection decode(UnsafeInput in, EncodeTag tag) throws IOException {
    GeographyCollection geo = new GeographyCollection();

    // Handle EMPTY flag
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty GeographyCollection.");
      return geo;
    }

    // Skip any covering data
    tag.skipCovering(in);

    try {
      int count = in.readInt();
      if (count < 0) {
        throw new IOException("GeographyCollection.decodeTagged error: negative count: " + count);
      }

      for (int i = 0; i < count; i++) {
        tag = EncodeTag.decode(in);
        // avoid creating new stream, directly call S2Geography.decode
        Geography feature = Geography.decode(in, tag);
        geo.features.add(feature);
      }
      geo.countShapes();

    } catch (EOFException e) {
      throw new IOException(
          "GeographyCollection.decodeTagged error: insufficient data to decode all parts of the geography.",
          e);
    }
    return geo;
  }

  void countShapes() {
    numShapesList.clear();
    totalShapes = 0;
    for (Geography geo : features) {
      int n = geo.numShapes();
      numShapesList.add(n);
      totalShapes += n;
    }
  }
}
