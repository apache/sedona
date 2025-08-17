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
package org.apache.sedona.common.jts2geojson;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.PrecisionModel;
import org.wololo.geojson.*;

public class GeoJSONReader {
  static final GeometryFactory FACTORY =
      new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING));

  public Geometry read(String json) {
    return read(json, null);
  }

  public Geometry read(String json, GeometryFactory geomFactory) {
    return read(GeoJSONFactory.create(json), geomFactory);
  }

  public Geometry read(GeoJSON geoJSON) {
    return read(geoJSON, null);
  }

  public Geometry read(GeoJSON geoJSON, GeometryFactory geomFactory) {
    var factory = geomFactory != null ? geomFactory : FACTORY;
    if (geoJSON instanceof Point) return convert((Point) geoJSON, factory);
    else if (geoJSON instanceof LineString) return convert((LineString) geoJSON, factory);
    else if (geoJSON instanceof Polygon) return convert((Polygon) geoJSON, factory);
    else if (geoJSON instanceof MultiPoint) return convert((MultiPoint) geoJSON, factory);
    else if (geoJSON instanceof MultiLineString) return convert((MultiLineString) geoJSON, factory);
    else if (geoJSON instanceof MultiPolygon) return convert((MultiPolygon) geoJSON, factory);
    else if (geoJSON instanceof GeometryCollection)
      return convert((GeometryCollection) geoJSON, factory);
    else throw new UnsupportedOperationException();
  }

  Geometry convert(Point point, GeometryFactory factory) {
    return factory.createPoint(convert(point.getCoordinates()));
  }

  Geometry convert(MultiPoint multiPoint, GeometryFactory factory) {
    return factory.createMultiPointFromCoords(convert(multiPoint.getCoordinates()));
  }

  Geometry convert(LineString lineString, GeometryFactory factory) {
    return factory.createLineString(convert(lineString.getCoordinates()));
  }

  Geometry convert(MultiLineString multiLineString, GeometryFactory factory) {
    var size = multiLineString.getCoordinates().length;
    var lineStrings = new org.locationtech.jts.geom.LineString[size];
    for (int i = 0; i < size; i++)
      lineStrings[i] = factory.createLineString(convert(multiLineString.getCoordinates()[i]));
    return factory.createMultiLineString(lineStrings);
  }

  Geometry convert(Polygon polygon, GeometryFactory factory) {
    return convertToPolygon(polygon.getCoordinates(), factory);
  }

  org.locationtech.jts.geom.Polygon convertToPolygon(
      double[][][] coordinates, GeometryFactory factory) {
    var shell = factory.createLinearRing(convert(coordinates[0]));
    if (coordinates.length > 1) {
      var size = coordinates.length - 1;
      var holes = new LinearRing[size];
      for (var i = 0; i < size; i++)
        holes[i] = factory.createLinearRing(convert(coordinates[i + 1]));
      return factory.createPolygon(shell, holes);
    } else {
      return factory.createPolygon(shell);
    }
  }

  Geometry convert(MultiPolygon multiPolygon, GeometryFactory factory) {
    var size = multiPolygon.getCoordinates().length;
    var polygons = new org.locationtech.jts.geom.Polygon[size];
    for (int i = 0; i < size; i++)
      polygons[i] = convertToPolygon(multiPolygon.getCoordinates()[i], factory);
    return factory.createMultiPolygon(polygons);
  }

  Geometry convert(GeometryCollection gc, GeometryFactory factory) {
    var size = gc.getGeometries().length;
    var geometries = new Geometry[size];
    for (var i = 0; i < size; i++) geometries[i] = read(gc.getGeometries()[i], factory);
    return factory.createGeometryCollection(geometries);
  }

  Coordinate convert(double[] c) {
    if (c.length == 2) {
      return new Coordinate(c[0], c[1]);
    } else if (c.length == 3) {
      return new Coordinate(c[0], c[1], c[2]);
    } else if (c.length == 4) {
      // Handle XYZM coordinates (4 values)
      return new CoordinateXYZM(c[0], c[1], c[2], c[3]);
    } else {
      return new Coordinate(c[0], c[1]);
    }
  }

  Coordinate[] convert(double[][] ca) {
    var coordinates = new Coordinate[ca.length];
    for (int i = 0; i < ca.length; i++) coordinates[i] = convert(ca[i]);
    return coordinates;
  }
}
