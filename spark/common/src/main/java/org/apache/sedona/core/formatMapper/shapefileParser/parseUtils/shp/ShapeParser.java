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

import java.io.Serializable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public abstract class ShapeParser implements Serializable {

  /** The geometry factory. */
  protected final GeometryFactory geometryFactory;

  /**
   * Instantiates a new shape parser.
   *
   * @param geometryFactory the geometry factory
   */
  protected ShapeParser(GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
  }

  /**
   * parse the shape to a geometry.
   *
   * @param reader the reader
   * @return the geometry
   */
  public abstract Geometry parseShape(ShapeReader reader);

  /**
   * According to the shapefile specification, any floating point number smaller than -10^38 is
   * considered a "no data" value. In practice, we also observe that ogrinfo considers 0 as a "no
   * data" value.
   *
   * @param value The coordinate value to check
   * @return true if the value is a "no data" value
   */
  protected static boolean isNoData(double value) {
    return value < -1e38 || value == 0;
  }

  /**
   * read numPoints of coordinates from input source.
   *
   * @param reader the reader
   * @param numPoints the num points
   * @return the coordinate array
   */
  protected Coordinate[] readCoordinates(ShapeReader reader, int numPoints) {
    Coordinate[] coordinates = new Coordinate[numPoints];

    for (int i = 0; i < numPoints; ++i) {
      double x = reader.readDouble();
      double y = reader.readDouble();
      coordinates[i] = new Coordinate(x, y);
    }

    return coordinates;
  }

  /**
   * Read coordinates with Z and M values from input source.
   *
   * @param reader the reader
   * @param numPoints the number of points
   * @return the coordinate array with Z and M values
   */
  protected Coordinate[] readCoordinatesWithZM(ShapeReader reader, int numPoints) {
    double[] x = new double[numPoints];
    double[] y = new double[numPoints];
    double[] z = new double[numPoints];
    double[] m = new double[numPoints];

    // Read all X and Y values
    for (int i = 0; i < numPoints; ++i) {
      x[i] = reader.readDouble();
      y[i] = reader.readDouble();
    }

    // Skip Z range (min/max)
    reader.skip(2 * ShapeFileConst.DOUBLE_LENGTH);

    // Read all Z values
    for (int i = 0; i < numPoints; ++i) {
      z[i] = reader.readDouble();
    }

    // Skip M range (min/max)
    reader.skip(2 * ShapeFileConst.DOUBLE_LENGTH);

    // Read all M values and check if any are valid data
    boolean allMNoData = true;
    for (int i = 0; i < numPoints; ++i) {
      m[i] = reader.readDouble();
      // Check if this is a valid M value (not "no data")
      if (!isNoData(m[i])) {
        allMNoData = false;
      }
    }

    // Create appropriate coordinate objects based on M values
    Coordinate[] coordinates = new Coordinate[numPoints];
    if (allMNoData) {
      // If all M values are nodata, use XYZ coordinates
      for (int i = 0; i < numPoints; ++i) {
        coordinates[i] = new Coordinate(x[i], y[i], z[i]);
      }
    } else {
      // If we have valid M values, use XYZM coordinates
      for (int i = 0; i < numPoints; ++i) {
        coordinates[i] = new CoordinateXYZM(x[i], y[i], z[i], m[i]);
      }
    }

    return coordinates;
  }

  /**
   * Read coordinates with M values from input source.
   *
   * @param reader the reader
   * @param numPoints the number of points
   * @return the coordinate array with M values
   */
  protected Coordinate[] readCoordinatesWithM(ShapeReader reader, int numPoints) {
    double[] x = new double[numPoints];
    double[] y = new double[numPoints];
    double[] m = new double[numPoints];

    // Read all X and Y values
    for (int i = 0; i < numPoints; ++i) {
      x[i] = reader.readDouble();
      y[i] = reader.readDouble();
    }

    // Skip M range (min/max)
    reader.skip(2 * ShapeFileConst.DOUBLE_LENGTH);

    // Read all M values
    boolean allMNoData = true;
    for (int i = 0; i < numPoints; ++i) {
      m[i] = reader.readDouble();
      // Check if this is a valid M value (not "no data")
      if (!isNoData(m[i])) {
        allMNoData = false;
      }
    }

    // Create appropriate coordinate objects based on M values
    Coordinate[] coordinates = new Coordinate[numPoints];
    if (allMNoData) {
      // If all M values are nodata, use XY coordinates
      for (int i = 0; i < numPoints; ++i) {
        coordinates[i] = new Coordinate(x[i], y[i]);
      }
    } else {
      // If we have valid M values, use XYM coordinates
      for (int i = 0; i < numPoints; ++i) {
        coordinates[i] = new CoordinateXYM(x[i], y[i], m[i]);
      }
    }

    return coordinates;
  }

  protected int[] readOffsets(ShapeReader reader, int numParts, int maxOffset) {
    int[] offsets = new int[numParts + 1];
    for (int i = 0; i < numParts; ++i) {
      offsets[i] = reader.readInt();
    }
    offsets[numParts] = maxOffset;

    return offsets;
  }
}
