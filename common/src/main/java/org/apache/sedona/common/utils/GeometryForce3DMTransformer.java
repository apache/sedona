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

public class GeometryForce3DMTransformer extends GeometryTransformer {

  private final double mValue;

  public GeometryForce3DMTransformer(double mValue) {
    this.mValue = mValue;
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    CoordinateXYM[] newCoords = new CoordinateXYM[coords.size()];
    for (int i = 0; i < coords.size(); i++) {
      Coordinate coordinate = coords.getCoordinate(i);
      newCoords[i] = new CoordinateXYM(coordinate.getX(), coordinate.getY(), mValue);
    }

    return createCoordinateSequence(newCoords);
  }

  public static Geometry transform(Geometry geometry, double mValue) {
    if (geometry.getCoordinates().length == 0) return geometry;
    if (Functions.hasM(geometry)) return geometry;

    GeometryForce3DMTransformer transformer = new GeometryForce3DMTransformer(mValue);
    return transformer.transform(geometry);
  }
}
