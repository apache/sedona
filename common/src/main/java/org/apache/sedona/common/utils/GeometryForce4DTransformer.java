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
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.GeometryTransformer;

public class GeometryForce4DTransformer extends GeometryTransformer {

  private static boolean hasZ;
  private static boolean hasM;
  private final double mValue;
  private final double zValue;

  public GeometryForce4DTransformer(double zValue, double mValue) {
    this.zValue = zValue;
    this.mValue = mValue;
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    CoordinateXYZM[] newCoords = new CoordinateXYZM[coords.size()];
    for (int i = 0; i < coords.size(); i++) {
      Coordinate coordinate = coords.getCoordinate(i);
      newCoords[i] =
          new CoordinateXYZM(
              coordinate.getX(),
              coordinate.getY(),
              hasZ ? coordinate.getZ() : zValue,
              hasM ? coordinate.getM() : mValue);
    }

    return createCoordinateSequence(newCoords);
  }

  public static Geometry transform(Geometry geometry, double zValue, double mValue) {
    if (geometry.getCoordinates().length == 0) return geometry;
    hasZ = Functions.hasZ(geometry);
    hasM = Functions.hasM(geometry);
    if (hasZ && hasM) return geometry;

    GeometryForce4DTransformer transformer = new GeometryForce4DTransformer(zValue, mValue);
    return transformer.transform(geometry);
  }
}
