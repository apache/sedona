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
package org.apache.sedona.common.geometryObjects;

import java.io.Serializable;
import java.util.Objects;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * Planar 2D bounding box with min/max X and Y. Empty boxes are encoded as {@code xmin > xmax} (JTS
 * convention), which makes union/expand a no-op against the empty value.
 */
public final class Box2D implements Serializable {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private final double xmin;
  private final double ymin;
  private final double xmax;
  private final double ymax;

  public Box2D(double xmin, double ymin, double xmax, double ymax) {
    this.xmin = xmin;
    this.ymin = ymin;
    this.xmax = xmax;
    this.ymax = ymax;
  }

  public static Box2D empty() {
    return new Box2D(
        Double.POSITIVE_INFINITY,
        Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY,
        Double.NEGATIVE_INFINITY);
  }

  public static Box2D fromGeometry(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
      return empty();
    }
    Envelope env = geometry.getEnvelopeInternal();
    return new Box2D(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
  }

  public static Box2D fromEnvelope(Envelope env) {
    if (env == null || env.isNull()) {
      return empty();
    }
    return new Box2D(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
  }

  public double getXMin() {
    return xmin;
  }

  public double getYMin() {
    return ymin;
  }

  public double getXMax() {
    return xmax;
  }

  public double getYMax() {
    return ymax;
  }

  public boolean isEmpty() {
    return xmin > xmax || ymin > ymax;
  }

  public Box2D expandToInclude(Box2D other) {
    if (other == null || other.isEmpty()) {
      return this;
    }
    if (this.isEmpty()) {
      return other;
    }
    return new Box2D(
        Math.min(xmin, other.xmin),
        Math.min(ymin, other.ymin),
        Math.max(xmax, other.xmax),
        Math.max(ymax, other.ymax));
  }

  public Envelope toEnvelope() {
    if (isEmpty()) {
      return new Envelope();
    }
    return new Envelope(xmin, xmax, ymin, ymax);
  }

  /** Convert to a closed polygon. Empty boxes return an empty polygon. */
  public Polygon toPolygon() {
    if (isEmpty()) {
      return GEOMETRY_FACTORY.createPolygon();
    }
    Coordinate[] coords =
        new Coordinate[] {
          new Coordinate(xmin, ymin),
          new Coordinate(xmin, ymax),
          new Coordinate(xmax, ymax),
          new Coordinate(xmax, ymin),
          new Coordinate(xmin, ymin)
        };
    return GEOMETRY_FACTORY.createPolygon(coords);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Box2D)) return false;
    Box2D other = (Box2D) o;
    if (this.isEmpty() && other.isEmpty()) return true;
    return Double.compare(xmin, other.xmin) == 0
        && Double.compare(ymin, other.ymin) == 0
        && Double.compare(xmax, other.xmax) == 0
        && Double.compare(ymax, other.ymax) == 0;
  }

  @Override
  public int hashCode() {
    if (isEmpty()) return 0;
    return Objects.hash(xmin, ymin, xmax, ymax);
  }

  @Override
  public String toString() {
    if (isEmpty()) return "BOX EMPTY";
    return "BOX(" + xmin + " " + ymin + ", " + xmax + " " + ymax + ")";
  }
}
