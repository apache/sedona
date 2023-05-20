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

import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import static java.lang.Math.abs;

public class Spheroid
{
    /**
     * Calculate the distance between two points on the earth using the Spheroid formula.
     * This algorithm does not use the radius of the earth, but instead uses the WGS84 ellipsoid.
     * This is similar to the Vincenty algorithm,but use the algorithm in
     * C. F. F. Karney, Algorithms for geodesics, J. Geodesy 87(1), 43–55 (2013)
     * It uses the implementation from GeographicLib so please expect a small difference
     * This will produce almost identical result to PostGIS ST_DistanceSpheroid and
     * PostGIS ST_Distance(useSpheroid=true)
     * @param geom1
     * @param geom2
     * @return
     */
    public static double distance(Geometry geom1, Geometry geom2) {
        Coordinate coordinate1 = geom1.getGeometryType().equals("Point")? geom1.getCoordinate():geom1.getCentroid().getCoordinate();
        Coordinate coordinate2 = geom2.getGeometryType().equals("Point")? geom2.getCoordinate():geom2.getCentroid().getCoordinate();
        // Calculate the distance between the two points
        double lat1 = coordinate1.getX();
        double lon1 = coordinate1.getY();
        double lat2 = coordinate2.getX();
        double lon2 = coordinate2.getY();
        GeodesicData g = Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2);
        return g.s12;
    }

    /**
     * Calculate the length of a geometry using the Spheroid formula.
     * Equivalent to PostGIS ST_LengthSpheroid and PostGIS ST_Length(useSpheroid=true)
     * WGS84 ellipsoid is used.
     * @param geom
     * @return
     */
    public static double length(Geometry geom) {
        if (geom.getGeometryType().equals("Polygon") || geom.getGeometryType().equals("LineString")) {
            PolygonArea p = new PolygonArea(Geodesic.WGS84, true);
            Coordinate[] coordinates = geom.getCoordinates();
            for (int i = 0; i < coordinates.length; i++) {
                p.AddPoint(coordinates[i].getX(), coordinates[i].getY());
            }
            PolygonResult compute = p.Compute();
            return compute.perimeter;
        }
        else if (geom.getGeometryType().equals("MultiPolygon") || geom.getGeometryType().equals("MultiLineString") || geom.getGeometryType().equals("GeometryCollection")) {
            double length = 0.0;
            for (int i = 0; i < geom.getNumGeometries(); i++) {
                length += length(geom.getGeometryN(i));
            }
            return length;
        }
        else {
            return 0.0;
        }
    }

    /**
     * Calculate the area of a geometry using the Spheroid formula.
     * Equivalent to PostGIS ST_Area(useSpheroid=true)
     * WGS84 ellipsoid is used.
     * @param geom
     * @return
     */
    public static double area(Geometry geom) {
        if (geom.getGeometryType().equals("Polygon")) {
            PolygonArea p = new PolygonArea(Geodesic.WGS84, false);
            Coordinate[] coordinates = geom.getCoordinates();
            for (int i = 0; i < coordinates.length; i++) {
                p.AddPoint(coordinates[i].getX(), coordinates[i].getY());
            }
            PolygonResult compute = p.Compute();
            // The area is negative if the polygon is oriented clockwise
            // We make sure that all area are positive
            return abs(compute.area);
        }
        else if (geom.getGeometryType().equals("MultiPolygon") || geom.getGeometryType().equals("GeometryCollection")) {
            double area = 0.0;
            for (int i = 0; i < geom.getNumGeometries(); i++) {
                area += area(geom.getGeometryN(i));
            }
            return area;
        }
        else {
            return 0.0;
        }
    }
}
