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
package org.apache.sedona.core.spatialOperator;

/**
 * Spatial predicates for range queries and join queries. Please refer to <a
 * href="https://en.wikipedia.org/wiki/DE-9IM#Spatial_predicates">Spatial predicates</a> for the
 * semantics of these spatial predicates.
 */
public enum SpatialPredicate {
  CONTAINS,
  INTERSECTS,
  WITHIN,
  COVERS,
  COVERED_BY,
  TOUCHES,
  OVERLAPS,
  CROSSES,
  EQUALS;

  /**
   * Get inverse predicate of given spatial predicate
   *
   * @param predicate spatial predicate
   * @return inverse predicate
   */
  public static SpatialPredicate inverse(SpatialPredicate predicate) {
    switch (predicate) {
      case CONTAINS:
        return SpatialPredicate.WITHIN;
      case WITHIN:
        return SpatialPredicate.CONTAINS;
      case COVERS:
        return SpatialPredicate.COVERED_BY;
      case COVERED_BY:
        return SpatialPredicate.COVERS;
      default:
        return predicate;
    }
  }
}
