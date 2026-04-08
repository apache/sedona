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

import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2Shape;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;

/**
 * A Geography implementation that stores WKB bytes as the primary representation, with lazy-parsed
 * JTS Geometry and S2 Geography caches. This enables zero-parse construction from WKB and deferred
 * S2 parsing only when spherical operations are needed.
 */
public class WKBGeography extends Geography {

  /**
   * When true, {@link #fromWKB(byte[], int)} eagerly builds the S2 Geography and ShapeIndex at
   * construction time instead of lazily on first access. This eliminates cold-path overhead for
   * predicate-heavy workloads (ST_Contains, ST_Intersects, etc.) at the cost of slower
   * deserialization for metric-only workloads.
   *
   * <p>Set via {@code spark.sedona.geography.eagerShapeIndex = true} or directly via {@link
   * #setEagerShapeIndex(boolean)}.
   *
   * <p>Default: {@code false} (lazy parsing).
   */
  private static volatile boolean eagerShapeIndex = false;

  /** Enable or disable eager ShapeIndex building at deserialization time. */
  public static void setEagerShapeIndex(boolean eager) {
    eagerShapeIndex = eager;
  }

  /** Returns whether eager ShapeIndex building is enabled. */
  public static boolean isEagerShapeIndex() {
    return eagerShapeIndex;
  }

  private final byte[] wkbBytes;

  // Lazy caches — volatile for thread safety with double-checked locking
  private volatile Geometry jtsGeometry;
  private volatile Geography s2Geography;
  private volatile ShapeIndexGeography shapeIndexGeography;

  private WKBGeography(byte[] wkbBytes, int srid) {
    super(GeographyKind.UNINITIALIZED);
    this.wkbBytes = wkbBytes;
    setSRID(srid);
  }

  /**
   * Create a WKBGeography from raw WKB bytes. When {@link #isEagerShapeIndex()} is false (default),
   * this is zero-parse — just wraps the byte array. When eager mode is enabled, this also parses
   * the WKB to S2 Geography and builds the ShapeIndex upfront.
   */
  public static WKBGeography fromWKB(byte[] wkb, int srid) {
    WKBGeography geog = new WKBGeography(wkb, srid);
    if (eagerShapeIndex) {
      // Pre-build S2 and ShapeIndex to eliminate cold-path overhead for predicates
      geog.getShapeIndexGeography();
    }
    return geog;
  }

  /** Create a WKBGeography from a JTS Geometry by serializing it to WKB. */
  public static WKBGeography fromJTS(Geometry jts) {
    org.locationtech.jts.io.WKBWriter writer =
        new org.locationtech.jts.io.WKBWriter(
            2, org.locationtech.jts.io.ByteOrderValues.BIG_ENDIAN);
    byte[] wkb = writer.write(jts);
    WKBGeography geog = new WKBGeography(wkb, jts.getSRID());
    geog.jtsGeometry = jts; // cache the JTS we already have
    return geog;
  }

  /** Create a WKBGeography from an existing S2 Geography by converting it to WKB. */
  public static WKBGeography fromS2Geography(Geography s2geog) {
    WKBWriter writer = new WKBWriter(2, org.locationtech.jts.io.ByteOrderValues.BIG_ENDIAN, false);
    byte[] wkb = writer.write(s2geog);
    WKBGeography geog = new WKBGeography(wkb, s2geog.getSRID());
    geog.s2Geography = s2geog; // cache the S2 we already have
    return geog;
  }

  /** Returns the raw WKB bytes. Zero cost. */
  public byte[] getWKBBytes() {
    return wkbBytes;
  }

  /** Returns a JTS Geometry, lazily parsed from WKB on first access. */
  public Geometry getJTSGeometry() {
    Geometry result = jtsGeometry;
    if (result == null) {
      synchronized (this) {
        result = jtsGeometry;
        if (result == null) {
          try {
            org.locationtech.jts.io.WKBReader reader = new org.locationtech.jts.io.WKBReader();
            result = reader.read(wkbBytes);
            result.setSRID(getSRID());
          } catch (ParseException e) {
            throw new RuntimeException("Failed to parse WKB to JTS Geometry", e);
          }
          jtsGeometry = result;
        }
      }
    }
    return result;
  }

  /**
   * Returns a ShapeIndexGeography wrapping the S2 Geography, lazily built on first access. The
   * ShapeIndex is cached for reuse across multiple predicate/distance operations on the same
   * object.
   */
  public ShapeIndexGeography getShapeIndexGeography() {
    ShapeIndexGeography result = shapeIndexGeography;
    if (result == null) {
      synchronized (this) {
        result = shapeIndexGeography;
        if (result == null) {
          result = new ShapeIndexGeography(getS2Geography());
          shapeIndexGeography = result;
        }
      }
    }
    return result;
  }

  /** Returns an S2 Geography, lazily parsed from WKB on first access. */
  public Geography getS2Geography() {
    Geography result = s2Geography;
    if (result == null) {
      synchronized (this) {
        result = s2Geography;
        if (result == null) {
          try {
            WKBReader reader = new WKBReader();
            result = reader.read(wkbBytes);
            result.setSRID(getSRID());
          } catch (ParseException e) {
            throw new RuntimeException("Failed to parse WKB to S2 Geography", e);
          }
          s2Geography = result;
        }
      }
    }
    return result;
  }

  // --- Geography abstract method delegation to lazy S2 ---

  @Override
  public int dimension() {
    return getS2Geography().dimension();
  }

  @Override
  public int numShapes() {
    return getS2Geography().numShapes();
  }

  @Override
  public S2Shape shape(int id) {
    return getS2Geography().shape(id);
  }

  @Override
  public S2Region region() {
    return getS2Geography().region();
  }

  @Override
  public void getCellUnionBound(List<com.google.common.geometry.S2CellId> cellIds) {
    getS2Geography().getCellUnionBound(cellIds);
  }

  @Override
  public String toString() {
    return getS2Geography().toString();
  }

  @Override
  public String toString(PrecisionModel precisionModel) {
    return getS2Geography().toString(precisionModel);
  }

  @Override
  public String toText(PrecisionModel precisionModel) {
    return getS2Geography().toText(precisionModel);
  }

  @Override
  public String toEWKT() {
    // Delegate to S2 but ensure SRID comes from this object
    Geography s2 = getS2Geography();
    s2.setSRID(getSRID());
    return s2.toEWKT();
  }

  @Override
  public String toEWKT(PrecisionModel precisionModel) {
    Geography s2 = getS2Geography();
    s2.setSRID(getSRID());
    return s2.toEWKT(precisionModel);
  }

  @Override
  public void encode(com.esotericsoftware.kryo.io.UnsafeOutput out, EncodeOptions opts)
      throws java.io.IOException {
    getS2Geography().encode(out, opts);
  }
}
