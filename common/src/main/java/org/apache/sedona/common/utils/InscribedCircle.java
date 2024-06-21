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

import org.apache.sedona.common.Functions;
import org.locationtech.jts.geom.Geometry;

public class InscribedCircle {
  public final Geometry center;
  public final Geometry nearest;
  public final double radius;

  public InscribedCircle(Geometry center, Geometry nearest, double radius) {
    this.center = center;
    this.nearest = nearest;
    this.radius = radius;
  }

  @Override
  public String toString() {
    return String.format(
        "---------------\ncenter: %s\nnearest: %s\nradius: %.20f\n---------------\n",
        Functions.asWKT(center), Functions.asWKT(nearest), radius);
  }

  public boolean equals(InscribedCircle other) {
    double epsilon = 1e-6;
    return this.center.equals(other.center)
        && this.nearest.equals(other.nearest)
        && Math.abs(this.radius - other.radius) < epsilon;
  }
}
