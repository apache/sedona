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
import org.locationtech.jts.geom.Geometry;

/**
 * Planar 3D bounding box with min/max X, Y, and Z. Storage order matches PostGIS {@code box3d}:
 * {@code xmin, ymin, zmin, xmax, ymax, zmax}.
 *
 * <p>Absence is represented by SQL NULL at the column level rather than an in-band sentinel.
 * Geometries that lack a Z dimension are treated as having {@code z = 0} (matching PostGIS), so the
 * bbox of an XY geometry has {@code zmin == zmax == 0} rather than NaN. Predicates require ordered
 * bounds ({@code xmin <= xmax}, {@code ymin <= ymax}, {@code zmin <= zmax}); inverted Z has no
 * defined planar meaning and there is no wraparound convention for the Z axis.
 */
public final class Box3D implements Serializable {

  private final double xmin;
  private final double ymin;
  private final double zmin;
  private final double xmax;
  private final double ymax;
  private final double zmax;

  public Box3D(double xmin, double ymin, double zmin, double xmax, double ymax, double zmax) {
    this.xmin = xmin;
    this.ymin = ymin;
    this.zmin = zmin;
    this.xmax = xmax;
    this.ymax = ymax;
    this.zmax = zmax;
  }

  /**
   * Returns the 3D bbox of {@code geometry}, or {@code null} for null/empty geometry. Z values that
   * are NaN (i.e. the coordinate has no Z dimension) are treated as 0, matching PostGIS's
   * convention where flat XY geometries get a degenerate Z extent at 0.
   */
  public static Box3D fromGeometry(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
      return null;
    }
    double xMin = Double.POSITIVE_INFINITY;
    double yMin = Double.POSITIVE_INFINITY;
    double zMin = Double.POSITIVE_INFINITY;
    double xMax = Double.NEGATIVE_INFINITY;
    double yMax = Double.NEGATIVE_INFINITY;
    double zMax = Double.NEGATIVE_INFINITY;
    boolean sawZ = false;
    for (Coordinate c : geometry.getCoordinates()) {
      xMin = Math.min(xMin, c.x);
      xMax = Math.max(xMax, c.x);
      yMin = Math.min(yMin, c.y);
      yMax = Math.max(yMax, c.y);
      double z = c.getZ();
      if (Double.isNaN(z)) {
        // PostGIS-compatible: missing Z is folded into the 0 plane on each coord.
        zMin = Math.min(zMin, 0.0);
        zMax = Math.max(zMax, 0.0);
      } else {
        sawZ = true;
        zMin = Math.min(zMin, z);
        zMax = Math.max(zMax, z);
      }
    }
    // If the geometry has no Z at any coordinate, collapse to z=0.
    if (!sawZ) {
      zMin = 0.0;
      zMax = 0.0;
    }
    return new Box3D(xMin, yMin, zMin, xMax, yMax, zMax);
  }

  public double getXMin() {
    return xmin;
  }

  public double getYMin() {
    return ymin;
  }

  public double getZMin() {
    return zmin;
  }

  public double getXMax() {
    return xmax;
  }

  public double getYMax() {
    return ymax;
  }

  public double getZMax() {
    return zmax;
  }

  /**
   * Returns the union of {@code this} and {@code other}. {@code other == null} is treated as a
   * no-op, returning {@code this}.
   */
  public Box3D expandToInclude(Box3D other) {
    if (other == null) {
      return this;
    }
    return new Box3D(
        Math.min(xmin, other.xmin),
        Math.min(ymin, other.ymin),
        Math.min(zmin, other.zmin),
        Math.max(xmax, other.xmax),
        Math.max(ymax, other.ymax),
        Math.max(zmax, other.zmax));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Box3D)) return false;
    Box3D other = (Box3D) o;
    return Double.compare(xmin, other.xmin) == 0
        && Double.compare(ymin, other.ymin) == 0
        && Double.compare(zmin, other.zmin) == 0
        && Double.compare(xmax, other.xmax) == 0
        && Double.compare(ymax, other.ymax) == 0
        && Double.compare(zmax, other.zmax) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(xmin, ymin, zmin, xmax, ymax, zmax);
  }

  @Override
  public String toString() {
    return "BOX3D(" + xmin + " " + ymin + " " + zmin + ", " + xmax + " " + ymax + " " + zmax + ")";
  }
}
