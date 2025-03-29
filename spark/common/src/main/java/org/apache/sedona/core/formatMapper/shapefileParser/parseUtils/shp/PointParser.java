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
package org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class PointParser extends ShapeParser {

  private final ShapeType shapeType;

  /**
   * create a parser that can abstract a Point from input source with given GeometryFactory.
   *
   * @param geometryFactory the geometry factory
   */
  public PointParser(GeometryFactory geometryFactory, ShapeType shapeType) {
    super(geometryFactory);
    this.shapeType = shapeType;
  }

  /**
   * abstract a Point shape.
   *
   * @param reader the reader
   * @return the geometry
   */
  @Override
  public Geometry parseShape(ShapeReader reader) {
    double x = reader.readDouble();
    double y = reader.readDouble();

    if (shapeType == ShapeType.POINTZ) {
      // For POINTZ, read both Z and M values
      double z = reader.readDouble();
      double m = reader.readDouble();
      if (isNoData(m)) {
        return geometryFactory.createPoint(new Coordinate(x, y, z));
      } else {
        return geometryFactory.createPoint(new CoordinateXYZM(x, y, z, m));
      }
    } else if (shapeType == ShapeType.POINTM) {
      // For POINTM, read M value only
      double m = reader.readDouble(); // M value read but not currently used
      if (isNoData(m)) {
        return geometryFactory.createPoint(new Coordinate(x, y));
      } else {
        return geometryFactory.createPoint(new CoordinateXYM(x, y, m));
      }
    } else {
      // Regular POINT with just XY coordinates
      return geometryFactory.createPoint(new Coordinate(x, y));
    }
  }
}
