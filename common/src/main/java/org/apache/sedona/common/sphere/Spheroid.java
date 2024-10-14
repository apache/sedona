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
package org.apache.sedona.common.sphere;

import static java.lang.Math.abs;

import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public class Spheroid {
  // Standard EPSG Codes
  public static final int EPSG_WORLD_MERCATOR = 3395;
  public static final int EPSG_NORTH_UTM_START = 32601;
  public static final int EPSG_NORTH_UTM_END = 32660;
  public static final int EPSG_NORTH_LAMBERT = 3574;
  public static final int EPSG_NORTH_STEREO = 3995;
  public static final int EPSG_SOUTH_UTM_START = 32701;
  public static final int EPSG_SOUTH_UTM_END = 32760;
  public static final int EPSG_SOUTH_LAMBERT = 3409;
  public static final int EPSG_SOUTH_STEREO = 3031;

  /**
   * Calculate the distance between two points on the earth using the Spheroid formula. This
   * algorithm does not use the radius of the earth, but instead uses the WGS84 ellipsoid. This is
   * similar to the Vincenty algorithm,but use the algorithm in C. F. F. Karney, Algorithms for
   * geodesics, J. Geodesy 87(1), 43–55 (2013) It uses the implementation from GeographicLib so
   * please expect a small difference This will produce almost identical result to PostGIS
   * ST_DistanceSpheroid and PostGIS ST_Distance(useSpheroid=true)
   *
   * @param geom1
   * @param geom2
   * @return
   */
  public static double distance(Geometry geom1, Geometry geom2) {
    Coordinate coordinate1 =
        geom1.getGeometryType().equals("Point")
            ? geom1.getCoordinate()
            : geom1.getCentroid().getCoordinate();
    Coordinate coordinate2 =
        geom2.getGeometryType().equals("Point")
            ? geom2.getCoordinate()
            : geom2.getCentroid().getCoordinate();
    // Calculate the distance between the two points
    double lon1 = coordinate1.getX();
    double lat1 = coordinate1.getY();
    double lon2 = coordinate2.getX();
    double lat2 = coordinate2.getY();
    GeodesicData g = Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2);
    return g.s12;
  }

  /**
   * Calculate the length of a geometry using the Spheroid formula. Equivalent to PostGIS
   * ST_LengthSpheroid and PostGIS ST_Length(useSpheroid=true) WGS84 ellipsoid is used.
   *
   * @param geom
   * @return
   */
  public static double length(Geometry geom) {
    String geomType = geom.getGeometryType();
    if (geomType.equals("Polygon") || geomType.equals("LineString")) {
      PolygonArea p = new PolygonArea(Geodesic.WGS84, true);
      Coordinate[] coordinates = geom.getCoordinates();
      for (int i = 0; i < coordinates.length; i++) {
        double lon = coordinates[i].getX();
        double lat = coordinates[i].getY();
        p.AddPoint(lat, lon);
      }
      PolygonResult compute = p.Compute();
      return compute.perimeter;
    } else if (geomType.equals("MultiPolygon")
        || geomType.equals("MultiLineString")
        || geomType.equals("GeometryCollection")) {
      double length = 0.0;
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        length += length(geom.getGeometryN(i));
      }
      return length;
    } else {
      return 0.0;
    }
  }

  /**
   * Calculate the area of a geometry using the Spheroid formula. Equivalent to PostGIS
   * ST_Area(useSpheroid=true) WGS84 ellipsoid is used.
   *
   * @param geom
   * @return
   */
  public static double area(Geometry geom) {
    String geomType = geom.getGeometryType();
    if (geomType.equals("Polygon")) {
      PolygonArea p = new PolygonArea(Geodesic.WGS84, false);
      Coordinate[] coordinates = geom.getCoordinates();
      for (int i = 0; i < coordinates.length; i++) {
        double lon = coordinates[i].getX();
        double lat = coordinates[i].getY();
        p.AddPoint(lat, lon);
      }
      PolygonResult compute = p.Compute();
      // The area is negative if the polygon is oriented clockwise
      // We make sure that all area are positive
      return abs(compute.area);
    } else if (geomType.equals("MultiPolygon") || geomType.equals("GeometryCollection")) {
      double area = 0.0;
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        area += area(geom.getGeometryN(i));
      }
      return area;
    } else {
      return 0.0;
    }
  }

  public static Double angularWidth(Envelope envelope) {
    double lon1 = envelope.getMinX();
    double lon2 = envelope.getMaxX();
    double lat =
        (envelope.getMinY() + envelope.getMaxY()) / 2; // Mid-latitude for width calculation

    // Compute geodesic distance
    GeodesicData g = Geodesic.WGS84.Inverse(lat, lon1, lat, lon2);
    double distance = g.s12; // Distance in meters

    // Convert distance to angular width in degrees
    Double angularWidth =
        Math.toDegrees(distance / (Geodesic.WGS84.EquatorialRadius() * Math.PI / 180));

    return angularWidth;
  }

  public static Double angularHeight(Envelope envelope) {
    double lat1 = envelope.getMinY();
    double lat2 = envelope.getMaxY();
    double lon =
        (envelope.getMinX() + envelope.getMaxX()) / 2; // Mid-longitude for height calculation

    // Compute geodesic distance
    GeodesicData g = Geodesic.WGS84.Inverse(lat1, lon, lat2, lon);
    double distance = g.s12; // Distance in meters

    // Convert distance to angular height in degrees
    Double angularHeight =
        Math.toDegrees(distance / (Geodesic.WGS84.EquatorialRadius() * Math.PI / 180));

    return angularHeight;
  }
}
