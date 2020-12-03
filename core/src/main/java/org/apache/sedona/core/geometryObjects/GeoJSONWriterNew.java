/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.core.geometryObjects;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.wololo.jts2geojson.GeoJSONReader;

public class GeoJSONWriterNew
{
    final static GeoJSONReader reader = new GeoJSONReader();

    public org.wololo.geojson.Geometry write(Geometry geometry) {
        Class<? extends Geometry> c = geometry.getClass();
        if (c.equals(Point.class)) {
            return convert((Point) geometry);
        } else if (c.equals(LineString.class)) {
            return convert((LineString) geometry);
        } else if (c.equals(Polygon.class)) {
            return convert((Polygon) geometry);
        } else if (c.equals(MultiPoint.class)) {
            return convert((MultiPoint) geometry);
        } else if (c.equals(MultiLineString.class)) {
            return convert((MultiLineString) geometry);
        } else if (c.equals(MultiPolygon.class)) {
            return convert((MultiPolygon) geometry);
        } else if (c.equals(GeometryCollection.class)) {
            return convert((GeometryCollection) geometry);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    org.wololo.geojson.Point convert(Point point) {
        org.wololo.geojson.Point json = new org.wololo.geojson.Point(
                convert(point.getCoordinate()));
        return json;
    }

    org.wololo.geojson.MultiPoint convert(MultiPoint multiPoint) {
        return new org.wololo.geojson.MultiPoint(
                convert(multiPoint.getCoordinates()));
    }

    org.wololo.geojson.LineString convert(LineString lineString) {
        return new org.wololo.geojson.LineString(
                convert(lineString.getCoordinates()));
    }

    org.wololo.geojson.MultiLineString convert(MultiLineString multiLineString) {
        int size = multiLineString.getNumGeometries();
        double[][][] lineStrings = new double[size][][];
        for (int i = 0; i < size; i++) {
            lineStrings[i] = convert(multiLineString.getGeometryN(i).getCoordinates());
        }
        return new org.wololo.geojson.MultiLineString(lineStrings);
    }

    org.wololo.geojson.Polygon convert(Polygon polygon) {
        int size = polygon.getNumInteriorRing() + 1;
        double[][][] rings = new double[size][][];
        rings[0] = convert(polygon.getExteriorRing().getCoordinates());
        for (int i = 0; i < size - 1; i++) {
            rings[i + 1] = convert(polygon.getInteriorRingN(i).getCoordinates());
        }
        return new org.wololo.geojson.Polygon(rings);
    }

    org.wololo.geojson.MultiPolygon convert(MultiPolygon multiPolygon) {
        int size = multiPolygon.getNumGeometries();
        double[][][][] polygons = new double[size][][][];
        for (int i = 0; i < size; i++) {
            polygons[i] = convert((Polygon) multiPolygon.getGeometryN(i)).getCoordinates();
        }
        return new org.wololo.geojson.MultiPolygon(polygons);
    }

    org.wololo.geojson.GeometryCollection convert(GeometryCollection gc) {
        int size = gc.getNumGeometries();
        org.wololo.geojson.Geometry[] geometries = new org.wololo.geojson.Geometry[size];
        for (int i = 0; i < size; i++) {
            geometries[i] = write((Geometry) gc.getGeometryN(i));
        }
        return new org.wololo.geojson.GeometryCollection(geometries);
    }

    double[] convert(Coordinate coordinate) {
        if(Double.isNaN( coordinate.getZ() )) {
            return new double[] { coordinate.x, coordinate.y };
        }
        else {
            return new double[] { coordinate.x, coordinate.y, coordinate.getZ() };
        }
    }

    double[][] convert(Coordinate[] coordinates) {
        double[][] array = new double[coordinates.length][];
        for (int i = 0; i < coordinates.length; i++) {
            array[i] = convert(coordinates[i]);
        }
        return array;
    }
}
