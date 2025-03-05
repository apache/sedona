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

import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

public class PolygonParser extends ShapeParser {

  private final ShapeType shapeType;

  /**
   * create a parser that can abstract a Polygon from input source with given GeometryFactory.
   *
   * @param geometryFactory the geometry factory
   */
  public PolygonParser(GeometryFactory geometryFactory, ShapeType shapeType) {
    super(geometryFactory);
    this.shapeType = shapeType;
  }

  /**
   * abstract abstract a Polygon shape.
   *
   * @param reader the reader
   * @return the geometry
   */
  @Override
  public Geometry parseShape(ShapeReader reader) {
    reader.skip(4 * ShapeFileConst.DOUBLE_LENGTH);

    int numRings = reader.readInt();
    int numPoints = reader.readInt();

    int[] offsets = readOffsets(reader, numRings, numPoints);

    // Read the coordinates for all rings
    Coordinate[] allCoordinates;

    if (shapeType == ShapeType.POLYGONZ) {
      allCoordinates = readCoordinatesWithZM(reader, numPoints);
    } else if (shapeType == ShapeType.POLYGONM) {
      allCoordinates = readCoordinatesWithM(reader, numPoints);
    } else {
      allCoordinates = readCoordinates(reader, numPoints);
    }

    boolean shellsCCW = false;

    LinearRing shell = null;
    List<LinearRing> holes = new ArrayList<>();
    List<Polygon> polygons = new ArrayList<>();

    for (int i = 0; i < numRings; ++i) {
      int startIndex = offsets[i];
      int endIndex = offsets[i + 1];
      int pointCount = endIndex - startIndex;

      if (pointCount <= 3) {
        continue; // if points less than 3, it's not a ring, we just abandon it
      }

      // Extract coordinates for this ring
      Coordinate[] ringCoordinates = new Coordinate[pointCount];
      System.arraycopy(allCoordinates, startIndex, ringCoordinates, 0, pointCount);

      LinearRing ring = geometryFactory.createLinearRing(ringCoordinates);

      if (shell == null) {
        shell = ring;
        shellsCCW = Orientation.isCCW(ringCoordinates);
      } else if (Orientation.isCCW(ringCoordinates) != shellsCCW) {
        holes.add(ring);
      } else {
        Polygon polygon =
            geometryFactory.createPolygon(shell, GeometryFactory.toLinearRingArray(holes));
        polygons.add(polygon);
        shell = ring;
        holes.clear();
      }
    }

    if (shell != null) {
      Polygon polygon =
          geometryFactory.createPolygon(shell, GeometryFactory.toLinearRingArray(holes));
      polygons.add(polygon);
    }

    if (polygons.size() == 1) {
      return polygons.get(0);
    }

    return geometryFactory.createMultiPolygon(polygons.toArray(new Polygon[0]));
  }
}
