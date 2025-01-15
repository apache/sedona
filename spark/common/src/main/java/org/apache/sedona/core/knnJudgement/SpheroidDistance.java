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

import org.apache.sedona.common.sphere.Spheroid;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;

public class SpheroidDistance implements ItemDistance {

  public SpheroidDistance() {}

  @Override
  public double distance(ItemBoundable item1, ItemBoundable item2) {
    if (item1 == item2) {
      return Double.MAX_VALUE;
    } else {
      Geometry g1 = (Geometry) item1.getItem();
      Geometry g2 = (Geometry) item2.getItem();
      return Spheroid.distance(g1, g2);
    }
  }

  public double distance(Geometry geometry1, Geometry geometry2) {
    if (geometry1 == geometry2) {
      return Double.MAX_VALUE;
    } else {
      return Spheroid.distance(geometry1, geometry2);
    }
  }
}
