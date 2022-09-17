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
package org.apache.sedona.common;

import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.GeometryGeoHashEncoder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.operation.distance3d.Distance3DOp;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.locationtech.jts.io.kml.KMLWriter;
import org.locationtech.jts.operation.valid.IsSimpleOp;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.wololo.jts2geojson.GeoJSONWriter;


public class Functions {
    public static double area(Geometry geometry) {
        return geometry.getArea();
    }

    public static double azimuth(Geometry left, Geometry right) {
        Coordinate leftCoordinate = left.getCoordinate();
        Coordinate rightCoordinate = right.getCoordinate();
        double deltaX = rightCoordinate.x - leftCoordinate.x;
        double deltaY = rightCoordinate.y - leftCoordinate.y;
        double azimuth = Math.atan2(deltaX, deltaY);
        return azimuth < 0 ? azimuth + (2 * Math.PI) : azimuth;
    }

    public static Geometry boundary(Geometry geometry) {
        return geometry.getBoundary();
    }

    public static Geometry buffer(Geometry geometry, double radius) {
        return geometry.buffer(radius);
    }

    public static Geometry envelope(Geometry geometry) {
        return geometry.getEnvelope();
    }

    public static double distance(Geometry left, Geometry right) {
        return left.distance(right);
    }

    public static double distance3d(Geometry left, Geometry right) {
        return new Distance3DOp(left, right).distance();
    }

    public static double length(Geometry geometry) {
        return geometry.getLength();
    }

    public static Geometry normalize(Geometry geometry) {
        geometry.normalize();
        return geometry;
    }

    public static Double x(Geometry geometry) {
        if (geometry instanceof Point) {
            return geometry.getCoordinate().x;
        }
        return null;
    }

    public static Double y(Geometry geometry) {
        if (geometry instanceof Point) {
            return geometry.getCoordinate().y;
        }
        return null;
    }

    public static Double z(Geometry geometry) {
        if (geometry instanceof Point) {
            return geometry.getCoordinate().z;
        }
        return null;
    }

    public static double xMin(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double min = Double.MAX_VALUE;
        for(int i=0; i < points.length; i++){
            min = Math.min(points[i].getX(), min);
        }
        return min;
    }
    
    public static double xMax(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double max = Double.MIN_VALUE;
        for (int i=0; i < points.length; i++) {
            max = Math.max(points[i].getX(), max);
        }
        return max;
    }

    public static double yMin(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double min = Double.MAX_VALUE;
        for(int i=0; i < points.length; i++){
            min = Math.min(points[i].getY(), min);
        }
        return min;
    }
    
    public static double yMax(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double max = Double.MIN_VALUE;
        for (int i=0; i < points.length; i++) {
            max = Math.max(points[i].getY(), max);
        }
        return max;
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS)
        throws FactoryException, TransformException {
        return transform(geometry, sourceCRS, targetCRS, false);
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS, boolean lenient)
        throws FactoryException, TransformException {
        CoordinateReferenceSystem sourceCRScode = CRS.decode(sourceCRS);
        CoordinateReferenceSystem targetCRScode = CRS.decode(targetCRS);
        MathTransform transform = CRS.findMathTransform(sourceCRScode, targetCRScode, lenient);
        return JTS.transform(geometry, transform);
    }

    public static Geometry flipCoordinates(Geometry geometry) {
        GeomUtils.flipCoordinates(geometry);
        return geometry;
    }

    public static String geohash(Geometry geometry, int precision) {
        return GeometryGeoHashEncoder.calculate(geometry, precision);   
    }

    public static Geometry pointOnSurface(Geometry geometry) {
        return GeomUtils.getInteriorPoint(geometry);
    }

    public static Geometry reverse(Geometry geometry) {
        return geometry.reverse();
    }

    public static Geometry geometryN(Geometry geometry, int n) {
        if (n < geometry.getNumGeometries()) {
            return geometry.getGeometryN(n);
        }
        return null;
    }

    public static Geometry interiorRingN(Geometry geometry, int n) {
        if (geometry instanceof Polygon) {
            Polygon polygon = (Polygon) geometry;
            if (n < polygon.getNumInteriorRing()) {
                return polygon.getInteriorRingN(n);
            }
        }
        return null;
    }

    public static Geometry pointN(Geometry geometry, int n) {
        if(!(geometry instanceof LineString)) {
            return null;
        }
        return GeomUtils.getNthPoint((LineString)geometry, n);
    }

    public static Geometry exteriorRing(Geometry geometry) {
        return GeomUtils.getExteriorRing(geometry);
    }

    public static String asEWKT(Geometry geometry) {
        return GeomUtils.getEWKT(geometry);
    }

    public static byte[] asEWKB(Geometry geometry) {
        return GeomUtils.getEWKB(geometry);
    }

    public static String asGeoJson(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        GeoJSONWriter writer = new GeoJSONWriter();
        return writer.write(geometry).toString();
    }

    public static int nPoints(Geometry geometry) {
        return geometry.getNumPoints();
    }

    public static int numGeometries(Geometry geometry) {
        return geometry.getNumGeometries();
    }

    public static Integer numInteriorRings(Geometry geometry) {
        if (geometry instanceof Polygon) {
            return ((Polygon) geometry).getNumInteriorRing();
        }
        return null;
    }

    public static String asGML(Geometry geometry) {
        return new GMLWriter().write(geometry);
    }

    public static String asKML(Geometry geometry) {
        return new KMLWriter().write(geometry);
    }

    public static Geometry force2D(Geometry geometry) {
        return GeomUtils.get2dGeom(geometry);
    }

    public static boolean isEmpty(Geometry geometry) {
        return geometry.isEmpty();
    }

    public static Geometry buildArea(Geometry geometry) {
        return GeomUtils.buildArea(geometry);
    }

    public static Geometry setSRID(Geometry geometry, int srid) {
        if (geometry == null) {
            return null;
        }
        GeometryFactory factory = new GeometryFactory(geometry.getPrecisionModel(), srid, geometry.getFactory().getCoordinateSequenceFactory());
        return factory.createGeometry(geometry);
    }

    public static int getSRID(Geometry geometry) {
        if (geometry == null) {
            return 0;
        }
        return geometry.getSRID();
    }

    public static boolean isClosed(Geometry geometry) {
        if (geometry instanceof Circle || geometry instanceof MultiPoint || geometry instanceof MultiPolygon || geometry instanceof Point || geometry instanceof Polygon) {
            return true;
        } else if (geometry instanceof LineString) {
            return ((LineString) geometry).isClosed();
        } else if (geometry instanceof MultiLineString) {
            return ((MultiLineString) geometry).isClosed();
        } else if (geometry instanceof GeometryCollection) {
            return false;
        }
        return false;
    }

    public static boolean isRing(Geometry geometry) {
        return geometry instanceof LineString && ((LineString) geometry).isClosed() && geometry.isSimple();
    }

    public static boolean isSimple(Geometry geometry) {
        return new IsSimpleOp(geometry).isSimple();
    }

    public static boolean isValid(Geometry geometry) {
        return new IsValidOp(geometry).isValid();
    }
}
