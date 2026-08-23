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

import org.locationtech.jts.coverage.CoveragePolygonValidator;
import org.locationtech.jts.geom.Geometry;

/** Validates one geometry's polygonal components against adjacent coverage members. */
public final class CoverageValidation {

  private CoverageValidation() {}

  /**
   * Returns the target boundary edges that make the target invalid in its coverage.
   *
   * <p>In addition to overlaps and mismatched boundaries, gaps up to {@code gapWidth} are treated
   * as invalid. The adjacent array should therefore include every geometry with polygonal
   * components within that distance of the target.
   *
   * @param target geometry to validate
   * @param adjacent geometries surrounding the target
   * @param gapWidth maximum width of gaps to report
   * @return target boundary edges causing coverage invalidity, or an empty line
   * @throws IllegalArgumentException if {@code gapWidth} is negative or non-finite
   */
  public static Geometry invalidEdges(Geometry target, Geometry[] adjacent, double gapWidth) {
    if (!Double.isFinite(gapWidth) || gapWidth < 0) {
      throw new IllegalArgumentException("gapWidth must be finite and non-negative");
    }
    Geometry invalidEdges = CoveragePolygonValidator.validate(target, adjacent, gapWidth);
    if (invalidEdges == null) {
      return null;
    }
    // GEOS coverage validation drops M while preserving Z. Normalize the JTS result to the same
    // layout and avoid heterogeneous XYZ/XYM components in a MultiLineString.
    return GeometryForce3DTransformer.transform(invalidEdges, Double.NaN);
  }

  /**
   * Returns the invalid target edges after ignoring one adjacent geometry structurally equal to the
   * target.
   *
   * <p>This helper is for candidate arrays that may contain the target row itself. It removes at
   * most one match using {@link Geometry#equalsExact(Geometry)}. Any additional exact matches stay
   * in the adjacent array so duplicate coverage members are still validated as overlaps.
   *
   * @param target geometry to validate
   * @param adjacent candidate geometries, which may include the target itself
   * @param gapWidth maximum width of gaps to report
   * @return target boundary edges causing coverage invalidity, or an empty line
   * @throws IllegalArgumentException if {@code gapWidth} is negative or non-finite
   */
  public static Geometry invalidEdgesIgnoringOneMatchingTarget(
      Geometry target, Geometry[] adjacent, double gapWidth) {
    if (target == null || adjacent == null) {
      return invalidEdges(target, adjacent, gapWidth);
    }

    for (int i = 0; i < adjacent.length; i++) {
      Geometry candidate = adjacent[i];
      if (candidate != null && target.equalsExact(candidate)) {
        Geometry[] neighbors = new Geometry[adjacent.length - 1];
        System.arraycopy(adjacent, 0, neighbors, 0, i);
        System.arraycopy(adjacent, i + 1, neighbors, i, adjacent.length - i - 1);
        return invalidEdges(target, neighbors, gapWidth);
      }
    }
    return invalidEdges(target, adjacent, gapWidth);
  }
}
