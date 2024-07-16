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

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.GeometryTransformer;

public class GeometryForce3DTransformer extends GeometryTransformer {

  private final double zValue;

  public GeometryForce3DTransformer(double zValue) {
    this.zValue = zValue;
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    Coordinate[] newCoords = new Coordinate[coords.size()];
    for (int i = 0; i < coords.size(); i++) {
      Coordinate coordinate = coords.getCoordinate(i);
      double z = coordinate.getZ();
      if (Double.isNaN(z)) {
        z = zValue;
      }
      newCoords[i] = new Coordinate(coordinate.getX(), coordinate.getY(), z);
    }

    return createCoordinateSequence(newCoords);
  }

  public static Geometry transform(Geometry geometry, double zValue) {
    if (geometry.getCoordinates().length == 0) return geometry;
    GeometryForce3DTransformer transformer = new GeometryForce3DTransformer(zValue);
    return transformer.transform(geometry);
  }
}
