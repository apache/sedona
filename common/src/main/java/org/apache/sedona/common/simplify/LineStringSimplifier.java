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
package org.apache.sedona.common.simplify;

import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class LineStringSimplifier extends BaseSimplifier {

  public static Geometry simplify(Geometry geom, boolean preserveCollapsed, double epsilon) {
    Coordinate[] simplified =
        CoordinatesSimplifier.simplifyInPlace(geom.getCoordinates(), epsilon, 2);

    GeometryFactory geometryFactory = geom.getFactory();
    if (simplified.length == 1) {
      if (preserveCollapsed)
        return geometryFactory.createLineString(ArrayUtils.addAll(simplified, simplified));
      else return geometryFactory.createLineString(simplified);
    } else if (simplified.length == 2 && !preserveCollapsed) {
      if (simplified[0] == simplified[1]) return geometryFactory.createLineString(simplified);
      else return geometryFactory.createLineString(simplified);
    } else {
      return geometryFactory.createLineString(simplified);
    }
  }
}
