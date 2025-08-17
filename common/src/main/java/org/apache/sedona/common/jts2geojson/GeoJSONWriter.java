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

import java.util.List;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;

public class GeoJSONWriter {

  static final GeoJSONReader reader = new GeoJSONReader();

  public org.wololo.geojson.Geometry write(Geometry geometry) {
    Class<? extends Geometry> c = geometry.getClass();
    if (c.equals(Point.class)) return convert((Point) geometry);
    else if (c.equals(LineString.class)) return convert((LineString) geometry);
    else if (c.equals(LinearRing.class)) return convert((LinearRing) geometry);
    else if (c.equals(Polygon.class)) return convert((Polygon) geometry);
    else if (c.equals(MultiPoint.class)) return convert((MultiPoint) geometry);
    else if (c.equals(MultiLineString.class)) return convert((MultiLineString) geometry);
    else if (c.equals(MultiPolygon.class)) return convert((MultiPolygon) geometry);
    else if (c.equals(GeometryCollection.class)) return convert((GeometryCollection) geometry);
    else throw new UnsupportedOperationException();
  }

  public org.wololo.geojson.FeatureCollection write(List<Feature> features) {
    var size = features.size();
    var featuresJson = new Feature[size];
    for (var i = 0; i < size; i++) featuresJson[i] = features.get(i);
    return new org.wololo.geojson.FeatureCollection(featuresJson);
  }

  org.wololo.geojson.Point convert(Point point) {
    return new org.wololo.geojson.Point(convert(point.getCoordinate()));
  }

  org.wololo.geojson.MultiPoint convert(MultiPoint multiPoint) {
    return new org.wololo.geojson.MultiPoint(convert(multiPoint.getCoordinates()));
  }

  org.wololo.geojson.LineString convert(LineString lineString) {
    return new org.wololo.geojson.LineString(convert(lineString.getCoordinates()));
  }

  org.wololo.geojson.LineString convert(LinearRing ringString) {
    return new org.wololo.geojson.LineString(convert(ringString.getCoordinates()));
  }

  org.wololo.geojson.MultiLineString convert(MultiLineString multiLineString) {
    var size = multiLineString.getNumGeometries();
    var lineStrings = new double[size][][];
    for (int i = 0; i < size; i++)
      lineStrings[i] = convert(multiLineString.getGeometryN(i).getCoordinates());
    return new org.wololo.geojson.MultiLineString(lineStrings);
  }

  org.wololo.geojson.Polygon convert(Polygon polygon) {
    var size = polygon.getNumInteriorRing() + 1;
    var rings = new double[size][][];
    rings[0] = convert(polygon.getExteriorRing().getCoordinates());
    for (int i = 0; i < size - 1; i++)
      rings[i + 1] = convert(polygon.getInteriorRingN(i).getCoordinates());
    return new org.wololo.geojson.Polygon(rings);
  }

  org.wololo.geojson.MultiPolygon convert(MultiPolygon multiPolygon) {
    var size = multiPolygon.getNumGeometries();
    var polygons = new double[size][][][];
    for (int i = 0; i < size; i++)
      polygons[i] = convert((Polygon) multiPolygon.getGeometryN(i)).getCoordinates();
    return new org.wololo.geojson.MultiPolygon(polygons);
  }

  org.wololo.geojson.GeometryCollection convert(GeometryCollection gc) {
    var size = gc.getNumGeometries();
    var geometries = new org.wololo.geojson.Geometry[size];
    for (int i = 0; i < size; i++) geometries[i] = write((Geometry) gc.getGeometryN(i));
    return new org.wololo.geojson.GeometryCollection(geometries);
  }

  double[] convert(Coordinate coordinate) {
    boolean hasZ = !Double.isNaN(coordinate.getZ());
    boolean hasM = !Double.isNaN(coordinate.getM());

    if (!hasZ && !hasM) {
      // XY case - only 2D coordinates
      return new double[] {coordinate.x, coordinate.y};
    } else if (hasZ && !hasM) {
      // XYZ case - 3D coordinates without measure
      return new double[] {coordinate.x, coordinate.y, coordinate.getZ()};
    } else if (hasZ && hasM) {
      // XYZM case - 3D coordinates with measure
      return new double[] {coordinate.x, coordinate.y, coordinate.getZ(), coordinate.getM()};
    } else {
      // XYM case - We don't support this directly
      throw new UnsupportedOperationException(
          "XYM coordinates are not supported. Please convert to XYZM coordinates by adding a Z value.");
    }
  }

  double[][] convert(Coordinate[] coordinates) {
    var array = new double[coordinates.length][];
    for (int i = 0; i < coordinates.length; i++) array[i] = convert(coordinates[i]);
    return array;
  }
}
