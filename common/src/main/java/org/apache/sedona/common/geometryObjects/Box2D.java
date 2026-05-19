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
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * Planar 2D bounding box with min/max X and Y. Always a valid finite bbox; absence of a bbox (e.g.
 * bbox of an empty geometry, extent over zero rows) is represented by SQL NULL at the column level
 * rather than by an in-band sentinel. This matches PostGIS behavior and leaves {@code xmin > xmax}
 * free for a future antimeridian-wraparound semantics on geography bboxes (cf. sedona-db's {@code
 * WraparoundInterval}).
 */
public final class Box2D implements Serializable {

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

  /** Returns the bbox of {@code geometry}, or {@code null} for null/empty geometry. */
  public static Box2D fromGeometry(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
      return null;
    }
    Envelope env = geometry.getEnvelopeInternal();
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

  /**
   * Returns the union of {@code this} and {@code other}. {@code other == null} is treated as a
   * no-op, returning {@code this}, so callers can fold over a stream that may include nulls.
   */
  public Box2D expandToInclude(Box2D other) {
    if (other == null) {
      return this;
    }
    return new Box2D(
        Math.min(xmin, other.xmin),
        Math.min(ymin, other.ymin),
        Math.max(xmax, other.xmax),
        Math.max(ymax, other.ymax));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Box2D)) return false;
    Box2D other = (Box2D) o;
    return Double.compare(xmin, other.xmin) == 0
        && Double.compare(ymin, other.ymin) == 0
        && Double.compare(xmax, other.xmax) == 0
        && Double.compare(ymax, other.ymax) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(xmin, ymin, xmax, ymax);
  }

  @Override
  public String toString() {
    return "BOX(" + xmin + " " + ymin + ", " + xmax + " " + ymax + ")";
  }
}
