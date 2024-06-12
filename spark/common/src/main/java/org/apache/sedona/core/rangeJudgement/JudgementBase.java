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
package org.apache.sedona.core.rangeJudgement;

import java.io.Serializable;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialOperator.SpatialPredicateEvaluators;
import org.locationtech.jts.geom.Geometry;

public class JudgementBase<U extends Geometry> implements Serializable {
  private final SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator;
  U queryGeometry;

  /**
   * Instantiates a new range filter using index.
   *
   * @param queryWindow the query window
   * @param spatialPredicate spatial predicate in query criteria {@code geom <spatialPredicate>
   *     queryWindow}
   */
  public JudgementBase(U queryWindow, SpatialPredicate spatialPredicate) {
    this.queryGeometry = queryWindow;
    this.evaluator = SpatialPredicateEvaluators.create(spatialPredicate);
  }

  /**
   * Instantiates a new range filter using index.
   *
   * @param queryWindow the query window
   * @param considerBoundaryIntersection the consider boundary intersection
   * @param leftCoveredByRight query window covered by geometry, or query window covers geometry.
   *     only effective when {@code considerBoundaryIntersection} was false
   */
  public JudgementBase(
      U queryWindow, boolean considerBoundaryIntersection, boolean leftCoveredByRight) {
    this(queryWindow, resolveSpatialPredicate(considerBoundaryIntersection, leftCoveredByRight));
  }

  public boolean match(Geometry spatialObject, Geometry queryWindow) {
    return evaluator.eval(spatialObject, queryWindow);
  }

  public static SpatialPredicate resolveSpatialPredicate(
      boolean considerBoundaryIntersection, boolean leftCoveredByRight) {
    if (considerBoundaryIntersection) {
      return SpatialPredicate.INTERSECTS;
    } else {
      return leftCoveredByRight ? SpatialPredicate.COVERED_BY : SpatialPredicate.COVERS;
    }
  }
}
