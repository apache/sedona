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
import com.google.common.geometry.*;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class PolygonGeography extends S2Geography {
  private static final Logger logger = Logger.getLogger(PolygonGeography.class.getName());

  public final S2Polygon polygon;

  public PolygonGeography() {
    super(GeographyKind.POLYGON);
    this.polygon = new S2Polygon();
  }

  public PolygonGeography(S2Polygon polygon) {
    super(GeographyKind.POLYGON);
    this.polygon = polygon;
  }

  @Override
  public int dimension() {
    return 2;
  }

  @Override
  public int numShapes() {
    return polygon.isEmpty() ? 0 : 1;
  }

  @Override
  public S2Shape shape(int id) {
    assert polygon != null;
    return polygon.shape();
  }

  @Override
  public S2Region region() {
    return this.polygon;
  }

  @Override
  public void getCellUnionBound(List<S2CellId> cellIds) {
    super.getCellUnionBound(cellIds);
  }

  @Override
  public void encode(UnsafeOutput out, EncodeOptions opts) throws IOException {
    // Encode polygon
    polygon.encode(out);
    out.flush();
  }

  /** This is what decodeTagged() actually calls */
  public static PolygonGeography decode(Input in, EncodeTag tag) throws IOException {
    // cast to UnsafeInput—will work if you always pass a Kryo-backed stream
    if (!(in instanceof UnsafeInput)) {
      throw new IllegalArgumentException("Expected UnsafeInput");
    }
    return decode((UnsafeInput) in, tag);
  }

  public static PolygonGeography decode(UnsafeInput in, EncodeTag tag) throws IOException {
    PolygonGeography geo = new PolygonGeography();

    // EMPTY
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty PolygonGeography.");
      return geo;
    }

    // 2) Skip past any covering cell-IDs written by encodeTagged
    tag.skipCovering(in);

    S2Polygon poly = S2Polygon.decode(in);
    geo = new PolygonGeography(poly);

    return geo;
  }
}
