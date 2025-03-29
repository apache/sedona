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

import java.io.IOException;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPoint;

public class MultiPointParser extends ShapeParser {

  private final ShapeType shapeType;

  /**
   * create a parser that can abstract a MultiPoint from input source with given GeometryFactory.
   *
   * @param geometryFactory the geometry factory
   */
  public MultiPointParser(GeometryFactory geometryFactory, ShapeType shapeType) {
    super(geometryFactory);
    this.shapeType = shapeType;
  }

  /**
   * abstract a MultiPoint shape.
   *
   * @param reader the reader
   * @return the geometry
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @Override
  public Geometry parseShape(ShapeReader reader) {
    reader.skip(4 * ShapeFileConst.DOUBLE_LENGTH);
    int numPoints = reader.readInt();

    Coordinate[] coordinates;

    if (shapeType == ShapeType.MULTIPOINTZ) {
      // Read XY coordinates, then Z and M values
      coordinates = readCoordinatesWithZM(reader, numPoints);
    } else if (shapeType == ShapeType.MULTIPOINTM) {
      // Read XY coordinates, then M values
      coordinates = readCoordinatesWithM(reader, numPoints);
    } else {
      // Standard XY coordinates
      coordinates = readCoordinates(reader, numPoints);
    }

    MultiPoint multiPoint = geometryFactory.createMultiPointFromCoords(coordinates);
    return multiPoint;
  }
}
