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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

public class PolyLineParser extends ShapeParser {

  private final ShapeType shapeType;

  /**
   * create a parser that can abstract a MultiPolyline from input source with given GeometryFactory.
   *
   * @param geometryFactory the geometry factory
   */
  public PolyLineParser(GeometryFactory geometryFactory, ShapeType shapeType) {
    super(geometryFactory);
    this.shapeType = shapeType;
  }

  /**
   * abstract a Polyline shape.
   *
   * @param reader the reader
   * @return the geometry
   */
  @Override
  public Geometry parseShape(ShapeReader reader) {
    reader.skip(4 * ShapeFileConst.DOUBLE_LENGTH);
    int numParts = reader.readInt();
    int numPoints = reader.readInt();

    int[] offsets = readOffsets(reader, numParts, numPoints);

    // Read all coordinates
    Coordinate[] allCoordinates;

    if (shapeType == ShapeType.POLYLINEZ) {
      allCoordinates = readCoordinatesWithZM(reader, numPoints);
    } else if (shapeType == ShapeType.POLYLINEM) {
      allCoordinates = readCoordinatesWithM(reader, numPoints);
    } else {
      allCoordinates = readCoordinates(reader, numPoints);
    }

    // Create line strings for each part
    LineString[] lines = new LineString[numParts];
    for (int i = 0; i < numParts; ++i) {
      int startIndex = offsets[i];
      int endIndex = offsets[i + 1];
      int pointCount = endIndex - startIndex;

      // Extract coordinates for this part
      Coordinate[] partCoordinates = new Coordinate[pointCount];
      System.arraycopy(allCoordinates, startIndex, partCoordinates, 0, pointCount);

      lines[i] = geometryFactory.createLineString(partCoordinates);
    }

    if (numParts == 1) {
      return lines[0];
    }

    return geometryFactory.createMultiLineString(lines);
  }
}
