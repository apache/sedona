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
package org.apache.sedona.common.utils;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.shape.fractal.HilbertCode;

/** Computes Hilbert-curve addresses for geometry envelope midpoints. */
public final class HilbertDistance {

  private HilbertDistance() {}

  /**
   * Computes the unsigned Hilbert-curve address of a geometry envelope's midpoint.
   *
   * <p>The midpoint is rescaled into the inclusive integer grid {@code [0, 2^level - 1]} for each
   * axis. Coordinates outside the supplied extent are clipped to the nearest edge. A zero-width
   * axis maps to coordinate zero.
   *
   * @param geometry input geometry
   * @param xmin minimum X value of the rescaling extent
   * @param ymin minimum Y value of the rescaling extent
   * @param xmax maximum X value of the rescaling extent
   * @param ymax maximum Y value of the rescaling extent
   * @param level curve level; values at or below zero map to address zero
   * @return unsigned 32-bit Hilbert address represented as a long
   * @throws IllegalArgumentException if the geometry is empty or the level exceeds 16
   */
  public static long compute(
      Geometry geometry, double xmin, double ymin, double xmax, double ymax, int level) {
    if (geometry.isEmpty()) {
      throw new IllegalArgumentException(
          "Hilbert distance cannot be computed for an empty geometry");
    }
    if (level > HilbertCode.MAX_LEVEL) {
      throw new IllegalArgumentException("Level out of range");
    }
    if (level <= 0) {
      return 0L;
    }

    Envelope envelope = geometry.getEnvelopeInternal();
    double xMidpoint = (envelope.getMinX() + envelope.getMaxX()) / 2.0;
    double yMidpoint = (envelope.getMinY() + envelope.getMaxY()) / 2.0;
    int sideLength = (1 << level) - 1;
    int x = scaleToGrid(xMidpoint, xmin, xmax, sideLength);
    int y = scaleToGrid(yMidpoint, ymin, ymax, sideLength);

    return Integer.toUnsignedLong(HilbertCode.encode(level, x, y));
  }

  private static int scaleToGrid(double value, double lower, double upper, int sideLength) {
    double width = upper - lower;
    if (width == 0.0) {
      return 0;
    }

    // Preserve the operation order used by GeoPandas so boundary rounding and truncation agree.
    double scaled = (value - lower) * (sideLength / width);
    if (Double.isNaN(scaled) || scaled <= 0.0) {
      return 0;
    }
    if (scaled >= sideLength) {
      return sideLength;
    }
    return (int) scaled;
  }
}
