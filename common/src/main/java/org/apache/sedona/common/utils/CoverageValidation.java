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
    return CoveragePolygonValidator.validate(target, adjacent, gapWidth);
  }
}
