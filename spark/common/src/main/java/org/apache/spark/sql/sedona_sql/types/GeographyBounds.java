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

import com.google.common.geometry.R1Interval;
import com.google.common.geometry.S1Angle;
import com.google.common.geometry.S1Interval;
import com.google.common.geometry.S2;
import com.google.common.geometry.S2EdgeUtil;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import java.util.ArrayList;
import java.util.List;

/**
 * The bounding box, in degrees of longitude and latitude, of geographies with edges on the sphere.
 * It is the box that the statistics of a Parquet GEOGRAPHY column hold: it has the edges of the
 * geographies and not only their vertices, it reaches a pole for a polygon around that pole, and
 * its western bound is above its eastern bound when it crosses the antimeridian.
 *
 * <p>A point is added from its coordinates, without an object for it. A line and a ring give their
 * vertices to the edge bounder of S2, which has the bounds of the edges between them. Only a ring
 * that spans half of all longitudes or more becomes a loop of S2, which tells which side of such a
 * ring is the inside and whether a pole is in it.
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
      addShapes(wkb, offset);
    } catch (RuntimeException | AssertionError e) {
      // Without the bounds of one geography there is no box for all of them
      unknown = true;
    }
  }

  private void addShapes(byte[] wkb, int offset) {
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
            shapes = shapes.union(edges(wkb, position + 4, count, width, little, false));
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
                shapes = shapes.union(ring(wkb, position + 4, count, width, little));
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
  }

  /**
   * The bounds of the edges between {@code count} vertices in a row, and with {@code close} of the
   * edge from the last of them to the first. A vertex with a coordinate that is not a number has no
   * edge, and gives its other coordinate as a point does.
   */
  private S2LatLngRect edges(
      byte[] wkb, int position, int count, int width, boolean little, boolean close) {
    S2EdgeUtil.RectBounder bounder = null;
    S2LatLng first = null;
    for (int at = position, end = position + count * width; at < end; at += width) {
      double longitude = readDouble(wkb, at, little);
      double latitude = readDouble(wkb, at + 8, little);
      if (Double.isNaN(longitude) || Double.isNaN(latitude)) {
        addPoint(longitude, latitude);
      } else {
        S2LatLng vertex = S2LatLng.fromDegrees(latitude, longitude);
        if (bounder == null) {
          bounder = new S2EdgeUtil.RectBounder();
          first = vertex;
        }
        bounder.addPoint(vertex);
      }
    }
    if (bounder == null) return S2LatLngRect.empty();
    if (close) bounder.addPoint(first);
    return bounder.getBound();
  }

  /** The bounds of the inside of a ring. */
  private S2LatLngRect ring(byte[] wkb, int position, int count, int width, boolean little) {
    // The last vertex of a ring of WKB repeats the first. Sedona's own writer of the WKB of a
    // geography leaves it out, and the ring still has the edge from its last vertex to its first.
    int last = position + (count - 1) * width;
    boolean open =
        readDouble(wkb, position, little) != readDouble(wkb, last, little)
            || readDouble(wkb, position + 8, little) != readDouble(wkb, last + 8, little);
    S2LatLngRect bound = edges(wkb, position, count, width, little, open);
    // The inside of a ring is its smaller side. With edges over less than half of all longitudes
    // that side has no pole, and it is within the bounds of the edges: the other side has every
    // meridian that the edges do not reach, which is more than half of the sphere.
    if (bound.lng().getLength() < HALF_OF_ALL_LONGITUDES) return bound;

    List<S2Point> vertices = new ArrayList<>(count);
    for (int at = position, end = position + count * width; at < end; at += width) {
      double longitude = readDouble(wkb, at, little);
      double latitude = readDouble(wkb, at + 8, little);
      if (Double.isNaN(longitude) || Double.isNaN(latitude)) continue;
      S2Point vertex = S2LatLng.fromDegrees(latitude, longitude).toPoint();
      // A loop of S2 has no vertex twice in a row
      if (vertices.isEmpty() || !vertex.equalsPoint(vertices.get(vertices.size() - 1))) {
        vertices.add(vertex);
      }
    }
    while (vertices.size() > 1 && vertices.get(vertices.size() - 1).equalsPoint(vertices.get(0))) {
      vertices.remove(vertices.size() - 1);
    }
    // Without three vertices a ring has no inside
    if (vertices.size() < 3) return bound;
    S2Loop loop = new S2Loop(vertices);
    double turning = loop.getTurningAngle();
    // The turning angle of a ring that halves the sphere is zero, and rounding gives it either
    // sign. Sedona does not read the inside of such a ring the same way everywhere, and its two
    // sides together are the sphere.
    if (Math.abs(turning) <= 4 * S2.getTurningAngleMaxError(vertices.size())) {
      return S2LatLngRect.full();
    }
    // The inside of a loop of S2 is on its left. A negative turning angle has the larger side
    // there.
    if (turning < 0) loop.invert();
    return loop.getRectBound();
  }

  /**
   * Adds the coordinates of a point. One that is not a number is left out by itself, as the Parquet
   * format has it, and both of them are no numbers for an empty point.
   */
  private void addPoint(double longitude, double latitude) {
    if (!Double.isNaN(longitude)) {
      if (longitude < 0) {
        if (longitude < westMin) westMin = longitude;
        if (longitude > westMax) westMax = longitude;
      } else {
        if (longitude < eastMin) eastMin = longitude;
        if (longitude > eastMax) eastMax = longitude;
      }
      // The antimeridian is longitude 180 and longitude -180
      if (longitude == 180 || longitude == -180) antimeridian = true;
    }
    if (!Double.isNaN(latitude)) {
      if (latitude < latMin) latMin = latitude;
      if (latitude > latMax) latMax = latitude;
    }
  }

  /**
   * The box of the geographies added since the last {@link #reset}.
   *
   * @return the western, eastern, southern and northern bound in this order, or null when the
   *     geographies have no longitude or no latitude, or one of them could not be read. The western
   *     bound is above the eastern bound for a box that crosses the antimeridian.
   */
  public double[] box() {
    if (unknown) return null;
    boolean hasWest = westMin <= westMax;
    boolean hasEast = eastMin <= eastMax;
    boolean longitudes = hasWest || hasEast;
    boolean latitudes = latMin <= latMax;
    double west = 0;
    double east = 0;
    if (longitudes) {
      west = hasWest ? westMin : eastMin;
      east = hasEast ? eastMax : westMax;
      // The other way around the sphere, across the antimeridian, when that is the shorter one
      if (hasWest && hasEast && (180 - eastMin) + (westMax + 180) < east - west) {
        west = eastMin;
        east = westMax;
      }
    }
    if (shapes.isEmpty()) {
      // A box has both dimensions
      if (!longitudes || !latitudes) return null;
      if (!antimeridian) return new double[] {west, east, latMin, latMax};
    }
    S2LatLngRect all = shapes;
    if (longitudes || latitudes) {
      // Points of which only one coordinate is a number widen that dimension alone
      R1Interval latitude =
          latitudes
              ? new R1Interval(radians(latMin, S2.M_PI_2), radians(latMax, S2.M_PI_2))
              : shapes.lat();
      S1Interval longitude = longitudes ? longitudes(west, east) : shapes.lng();
      all = all.union(new S2LatLngRect(latitude, longitude));
    }
    return degrees(all);
  }

  /**
   * Whether two boxes have a point in common. A box is the western, eastern, southern and northern
   * bound in degrees, with the western bound above the eastern bound across the antimeridian, as
   * {@link #box} gives it and as the statistics of a Parquet GEOGRAPHY column have it. Longitude
   * 180 and longitude -180 are one meridian.
   */
  public static boolean intersects(double[] first, double[] second) {
    return rectangle(first).intersects(rectangle(second));
  }

  /**
   * The box of every point that is within {@code radians} of a point of {@code box}, as an angle on
   * the sphere. It is not smaller than that.
   */
  public static double[] expand(double[] box, double radians) {
    return degrees(rectangle(box).expandedByDistance(S1Angle.radians(radians)));
  }

  private static S2LatLngRect rectangle(double[] box) {
    return new S2LatLngRect(
        new R1Interval(radians(box[2], S2.M_PI_2), radians(box[3], S2.M_PI_2)),
        longitudes(box[0], box[1]));
  }

  /** The box of a rectangle of S2, which is a little larger than the rectangle. */
  private static double[] degrees(S2LatLngRect rectangle) {
    S2LatLngRect all = rectangle.expanded(S2LatLng.fromRadians(MARGIN, MARGIN));
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

  /** The longitudes from {@code west} eastwards to {@code east}, both in degrees. */
  private static S1Interval longitudes(double west, double east) {
    double lo = radians(west, S2.M_PI);
    double hi = radians(east, S2.M_PI);
    // From longitude 180 to longitude -180 is the antimeridian alone, which points on both of
    // them give. S2 reads an interval from pi to -pi as one without a longitude.
    if (lo == S2.M_PI && hi == -S2.M_PI) return S1Interval.fromPoint(S2.M_PI);
    return new S1Interval(lo, hi);
  }

  private static double radians(double degrees, double limit) {
    return Math.max(-limit, Math.min(limit, Math.toRadians(degrees)));
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
}
