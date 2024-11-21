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
package org.apache.sedona.common.spider;

import java.util.Random;
import org.locationtech.jts.geom.Coordinate;

/** Generates points or boxes that are distributed according to the Sierpinski distribution */
public class SierpinskiGenerator extends PointBasedGenerator {
  private final Coordinate point1;
  private final Coordinate point2;
  private final Coordinate point3;
  private Coordinate prevPoint;

  private long iRecord = 0;

  public SierpinskiGenerator(Random random, PointBasedParameter pointBasedParameter) {
    super(random, pointBasedParameter);
    // Initialize the three vertices of the triangle
    point1 = new Coordinate(0.0, 0.0);
    point2 = new Coordinate(1.0, 0.0);
    point3 = new Coordinate(0.5, Math.sqrt(3) / 2);
  }

  @Override
  protected Coordinate generateCoordinate() {
    Coordinate point;
    if (iRecord == 0) {
      point = point1;
    } else if (iRecord == 1) {
      point = point2;
    } else if (iRecord == 2) {
      point = point3;
    } else {
      // Roll a die (1-5) and choose which vertex to use
      int roll = dice(5);
      Coordinate targetPoint;
      if (roll <= 2) {
        targetPoint = point1;
      } else if (roll <= 4) {
        targetPoint = point2;
      } else {
        targetPoint = point3;
      }
      point = middlePoint(prevPoint, targetPoint);
    }

    iRecord++;
    prevPoint = point;
    return point;
  }

  private Coordinate middlePoint(Coordinate p1, Coordinate p2) {
    double x = (p1.x + p2.x) / 2;
    double y = (p1.y + p2.y) / 2;
    return new Coordinate(x, y);
  }
}
