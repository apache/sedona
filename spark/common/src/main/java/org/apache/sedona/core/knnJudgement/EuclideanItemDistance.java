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
package org.apache.sedona.core.knnJudgement;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;

public class EuclideanItemDistance implements ItemDistance {

  public EuclideanItemDistance() {}

  @Override
  public double distance(ItemBoundable item1, ItemBoundable item2) {
    if (item1 == item2) {
      return Double.MAX_VALUE;
    } else {
      Geometry g1 = (Geometry) item1.getItem();
      Geometry g2 = (Geometry) item2.getItem();
      return distanceOrInfinity(g1, g2);
    }
  }

  public double distance(Geometry geometry1, Geometry geometry2) {
    if (geometry1 == geometry2) {
      return Double.MAX_VALUE;
    } else {
      return distanceOrInfinity(geometry1, geometry2);
    }
  }

  /**
   * JTS returns 0 as the distance to an empty geometry, which would make an empty query geometry
   * pick arbitrary nearest neighbors. Rank such pairs as infinitely far apart instead, so that an
   * empty query geometry gets no neighbors and an empty object geometry is never picked, as with
   * the sphere and spheroid metrics.
   */
  private static double distanceOrInfinity(Geometry geometry1, Geometry geometry2) {
    if (geometry1.isEmpty() || geometry2.isEmpty()) {
      return Double.POSITIVE_INFINITY;
    }
    return geometry1.distance(geometry2);
  }
}
