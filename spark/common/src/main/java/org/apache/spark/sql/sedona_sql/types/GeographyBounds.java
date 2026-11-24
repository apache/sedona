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
package org.apache.spark.sql.sedona_sql.types;

import com.google.common.geometry.S2EdgeUtil;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import java.io.IOException;
import org.apache.sedona.common.S2Geography.WKBReader;
import org.locationtech.jts.io.InStream;

/**
 * The bounding box, in degrees of longitude and latitude, of geographies with edges on the sphere.
 * It is the box that the statistics of a Parquet GEOGRAPHY column hold: it has the edges of the
 * geographies and not only their vertices, it reaches a pole for a polygon around that pole, and
 * its western bound is above its eastern bound when it crosses the antimeridian.
 *
 * <p>A point is added from its coordinates, without an object for it. A line and a ring give their
 * vertices to the edge bounder of S2, which has the bounds of the edges between them. Only a
 * polygon whose ring spans half of all longitudes or more is read into its S2 form, which tells
 * which side of such a ring is the inside and whether a pole is in it.
 */
public final class GeographyBounds {
  /**
   * What the box grows by on every side, in radians, when S2 has computed any of it. S2 works in
   * radians, and a coordinate that goes from degrees to radians and back can come out some 1e-15
   * radians off. The margin keeps every coordinate inside the box as it is written.
   */
  private static final double MARGIN = 1e-14;

  /** A little less than pi, so that rounding keeps a ring of half of all longitudes above it. */
  private static final double HALF_OF_ALL_LONGITUDES = Math.PI - 1e-9;

  private final WKBReader reader = new WKBReader();
  private final Slice slice = new Slice();

  // The points, by the sign of their longitude. The two ranges give the box of the points both
  // ways around the sphere, in the coordinates that the points have.
  private double westMin;
  private double westMax;
  private double eastMin;
  private double eastMax;
  private double latMin;
  private double latMax;
  private boolean antimeridian;
  // The other geographies
  private S2LatLngRect shapes;
  private boolean unknown;

  public GeographyBounds() {
    reset();
  }

  /** Forgets the geographies added so far. */
  public void reset() {
    westMin = Double.POSITIVE_INFINITY;
    westMax = Double.NEGATIVE_INFINITY;
    eastMin = Double.POSITIVE_INFINITY;
    eastMax = Double.NEGATIVE_INFINITY;
    latMin = Double.POSITIVE_INFINITY;
    latMax = Double.NEGATIVE_INFINITY;
    antimeridian = false;
    shapes = S2LatLngRect.empty();
    unknown = false;
  }

  /**
   * Adds the geography that {@code wkb} holds as ISO WKB from {@code offset} to its end. The WKB is
   * well-formed: {@link GeographyWkbScanner#scan} has walked it.
   */
  public void add(byte[] wkb, int offset) {
    if (unknown) return;
    try {
      if (!addShapes(wkb, offset)) {
        slice.set(wkb, offset);
        shapes = shapes.union(reader.read(slice).region().getRectBound());
      }
    } catch (Exception | AssertionError e) {
      // Without the bounds of one geography there is no box for all of them
      unknown = true;
    }
  }

  /**
   * Adds the points, lines and polygons of a geography from their coordinates.
   *
   * @return false for a geography that has to be read into its S2 form instead
   */
  private boolean addShapes(byte[] wkb, int offset) {
    int position = offset;
    long pending = 1;
    while (pending-- > 0) {
      boolean little = wkb[position] == 1;
      int code = readInt(wkb, position + 1, little);
      int layout = code / 1000;
      int width = (layout == 0 ? 2 : layout == 3 ? 4 : 3) * 8;
      position += 5;
      switch (code % 1000) {
        case 1:
          addPoint(readDouble(wkb, position, little), readDouble(wkb, position + 8, little));
          position += width;
          break;
        case 2:
          {
            int count = readInt(wkb, position, little);
            shapes = shapes.union(edges(wkb, position + 4, count, width, little));
            position += 4 + count * width;
            break;
          }
        case 3:
          {
            int rings = readInt(wkb, position, little);
            position += 4;
            for (int ring = 0; ring < rings; ring++) {
              int count = readInt(wkb, position, little);
              // The polygon is within its first ring. The other rings are holes in it.
              if (ring == 0 && count > 0) {
                S2LatLngRect bound = edges(wkb, position + 4, count, width, little);
                // The inside of a ring is its smaller side. With edges over less than half of
                // all longitudes that side has no pole, and it is within the bounds of the
                // edges: the other side has every meridian that the edges do not reach, which
                // is more than half of the sphere.
                if (bound.lng().getLength() >= HALF_OF_ALL_LONGITUDES) return false;
                shapes = shapes.union(bound);
              }
              position += 4 + count * width;
            }
            break;
          }
        default:
          pending += readInt(wkb, position, little);
          position += 4;
      }
    }
    return true;
  }

  /** The bounds of the edges between {@code count} vertices in a row. */
  private static S2LatLngRect edges(
      byte[] wkb, int position, int count, int width, boolean little) {
    if (count == 0) return S2LatLngRect.empty();
    S2EdgeUtil.RectBounder bounder = new S2EdgeUtil.RectBounder();
    for (int at = position, end = position + count * width; at < end; at += width) {
      bounder.addPoint(
          S2LatLng.fromDegrees(readDouble(wkb, at + 8, little), readDouble(wkb, at, little)));
    }
    return bounder.getBound();
  }

  private void addPoint(double longitude, double latitude) {
    // The coordinates of an empty point are not numbers
    if (Double.isNaN(longitude) || Double.isNaN(latitude)) return;
    if (longitude < 0) {
      if (longitude < westMin) westMin = longitude;
      if (longitude > westMax) westMax = longitude;
    } else {
      if (longitude < eastMin) eastMin = longitude;
      if (longitude > eastMax) eastMax = longitude;
    }
    if (latitude < latMin) latMin = latitude;
    if (latitude > latMax) latMax = latitude;
    // The antimeridian is longitude 180 and longitude -180
    if (longitude == 180 || longitude == -180) antimeridian = true;
  }

  /**
   * The box of the geographies added since the last {@link #reset}.
   *
   * @return the western, eastern, southern and northern bound in this order, or null when no
   *     geography with a coordinate was added or one of them could not be read. The western bound
   *     is above the eastern bound for a box that crosses the antimeridian.
   */
  public double[] box() {
    if (unknown) return null;
    boolean points = latMin <= latMax;
    if (!points && shapes.isEmpty()) return null;
    double west = 0;
    double east = 0;
    if (points) {
      boolean hasWest = westMin <= westMax;
      boolean hasEast = eastMin <= eastMax;
      west = hasWest ? westMin : eastMin;
      east = hasEast ? eastMax : westMax;
      // The other way around the sphere, across the antimeridian, when that is the shorter one
      if (hasWest && hasEast && (180 - eastMin) + (westMax + 180) < east - west) {
        west = eastMin;
        east = westMax;
      }
      if (shapes.isEmpty() && !antimeridian) return new double[] {west, east, latMin, latMax};
    }
    S2LatLngRect all = shapes;
    if (points) {
      all =
          all.union(
              new S2LatLngRect(
                  S2LatLng.fromDegrees(latMin, west), S2LatLng.fromDegrees(latMax, east)));
    }
    all = all.expanded(S2LatLng.fromRadians(MARGIN, MARGIN));
    double south = Math.max(-90, all.latLo().degrees());
    double north = Math.min(90, all.latHi().degrees());
    if (all.lng().isFull()) return new double[] {-180, 180, south, north};
    return new double[] {
      Math.max(-180, Math.min(180, all.lngLo().degrees())),
      Math.max(-180, Math.min(180, all.lngHi().degrees())),
      south,
      north
    };
  }

  private static int readInt(byte[] bytes, int at, boolean little) {
    int value = 0;
    for (int i = 0; i < 4; i++) {
      value = value << 8 | (bytes[little ? at + 3 - i : at + i] & 0xff);
    }
    return value;
  }

  private static double readDouble(byte[] bytes, int at, boolean little) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value = value << 8 | (bytes[little ? at + 7 - i : at + i] & 0xffL);
    }
    return Double.longBitsToDouble(value);
  }

  /** The bytes of an array from an offset on, for a reader of WKB. */
  private static final class Slice implements InStream {
    private byte[] bytes;
    private int position;

    void set(byte[] bytes, int offset) {
      this.bytes = bytes;
      this.position = offset;
    }

    @Override
    public int read(byte[] buffer) throws IOException {
      if (buffer.length > bytes.length - position) {
        throw new IOException("The WKB ends before the geography does");
      }
      System.arraycopy(bytes, position, buffer, 0, buffer.length);
      position += buffer.length;
      return buffer.length;
    }
  }
}
