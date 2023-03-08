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

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2RegionCoverer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.GeometryGeoHashEncoder;
import org.apache.sedona.common.utils.GeometrySplitter;
import org.apache.sedona.common.utils.S2Utils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.algorithm.MinimumBoundingCircle;
import org.locationtech.jts.algorithm.hull.ConcaveHull;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.locationtech.jts.io.kml.KMLWriter;
import org.locationtech.jts.linearref.LengthIndexedLine;
import org.locationtech.jts.operation.distance3d.Distance3DOp;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.operation.valid.IsSimpleOp;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class Functions {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static Geometry EMPTY_POLYGON = GEOMETRY_FACTORY.createPolygon(null, null);
    private static GeometryCollection EMPTY_GEOMETRY_COLLECTION = GEOMETRY_FACTORY.createGeometryCollection(null);

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
        Geometry boundary = geometry.getBoundary();
        if (boundary instanceof LinearRing) {
            boundary = GEOMETRY_FACTORY.createLineString(boundary.getCoordinates());
        }
        return boundary;
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
        double max = - Double.MAX_VALUE;
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
        double max = - Double.MAX_VALUE;
        for (int i=0; i < points.length; i++) {
            max = Math.max(points[i].getY(), max);
        }
        return max;
    }

    public static Double zMax(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double max = - Double.MAX_VALUE;
        for (int i=0; i < points.length; i++) {
            if(java.lang.Double.isNaN(points[i].getZ()))
                continue;
            max = Math.max(points[i].getZ(), max);
        }
        return max == -Double.MAX_VALUE ? null : max;
    }

    public static Double zMin(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        double min = Double.MAX_VALUE;
        for(int i=0; i < points.length; i++){
            if(java.lang.Double.isNaN(points[i].getZ()))
                continue;
            min = Math.min(points[i].getZ(), min);
        }
        return min == Double.MAX_VALUE ? null : min;
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS)
        throws FactoryException, TransformException {
        return transform(geometry, sourceCRS, targetCRS, false);
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS, boolean lenient)
        throws FactoryException, TransformException {

        CoordinateReferenceSystem sourceCRScode = parseCRSString(sourceCRS);
        CoordinateReferenceSystem targetCRScode = parseCRSString(targetCRS);
        MathTransform transform = CRS.findMathTransform(sourceCRScode, targetCRScode, lenient);
        return JTS.transform(geometry, transform);
    }

    private static CoordinateReferenceSystem parseCRSString(String CRSString)
            throws FactoryException
    {
        try {
            return CRS.decode(CRSString);
        }
        catch (NoSuchAuthorityCodeException e) {
            try {
                return CRS.parseWKT(CRSString);
            }
            catch (FactoryException ex) {
                throw new FactoryException("First failed to read as a well-known CRS code: \n" + e.getMessage() + "\nThen failed to read as a WKT CRS string: \n" + ex.getMessage());
            }
        }
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
                Geometry interiorRing = polygon.getInteriorRingN(n);
                if (interiorRing instanceof LinearRing) {
                    interiorRing = GEOMETRY_FACTORY.createLineString(interiorRing.getCoordinates());
                }
                return interiorRing;
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
        Geometry ring = GeomUtils.getExteriorRing(geometry);
        if (ring instanceof LinearRing) {
            ring = GEOMETRY_FACTORY.createLineString(ring.getCoordinates());
        }
        return ring;
    }

    public static String asEWKT(Geometry geometry) {
        return GeomUtils.getEWKT(geometry);
    }

    public static String asWKT(Geometry geometry) {
        return GeomUtils.getWKT(geometry);
    }

    public static byte[] asEWKB(Geometry geometry) {
        return GeomUtils.getEWKB(geometry);
    }

    public static byte[] asWKB(Geometry geometry) {
        return GeomUtils.getWKB(geometry);
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

    public static int nDims(Geometry geometry) {
        int count_dimension =0;
        Coordinate geom = geometry.getCoordinate();
        Double x_cord = geom.getX();
        Double y_cord = geom.getY();
        Double z_cord = geom.getZ();
        Double m_cord = geom.getM();
        if(!java.lang.Double.isNaN(x_cord))
            count_dimension++;
        if(!java.lang.Double.isNaN(y_cord))
            count_dimension++;
        if(!java.lang.Double.isNaN(z_cord))
            count_dimension++;
        if(!java.lang.Double.isNaN(m_cord))
            count_dimension++;
        return count_dimension;
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

    public static Geometry addPoint(Geometry linestring, Geometry point) {
        return addPoint(linestring, point, -1);
    }

    public static Geometry addPoint(Geometry linestring, Geometry point, int position) {
        if (linestring instanceof LineString && point instanceof Point) {
            List<Coordinate> coordinates = new ArrayList<>(Arrays.asList(linestring.getCoordinates()));
            if (-1 <= position && position <= coordinates.size()) {
                if (position < 0) {
                    coordinates.add(point.getCoordinate());
                } else {
                    coordinates.add(position, point.getCoordinate());
                }
                return GEOMETRY_FACTORY.createLineString(coordinates.toArray(new Coordinate[0]));
            }
        }
        return null;
    }

    public static Geometry removePoint(Geometry linestring) {
        if (linestring != null) {
            return removePoint(linestring, -1);
        }
        return null;
    }

    public static Geometry removePoint(Geometry linestring, int position) {
        if (linestring instanceof LineString) {
            List<Coordinate> coordinates = new ArrayList<>(Arrays.asList(linestring.getCoordinates()));
            if (2 < coordinates.size() && position < coordinates.size()) {
                if (position == -1) {
                    position = coordinates.size() - 1;
                }
                coordinates.remove(position);
                return GEOMETRY_FACTORY.createLineString(coordinates.toArray(new Coordinate[0]));
            }
        }
        return null;
    }

    public static Geometry setPoint(Geometry linestring, int position, Geometry point) {
        if (linestring instanceof LineString) {
            List<Coordinate> coordinates = new ArrayList<>(Arrays.asList(linestring.getCoordinates()));
            if (-coordinates.size() <= position && position < coordinates.size()) {
                if (position < 0) {
                    coordinates.set(coordinates.size() + position, point.getCoordinate());
                } else {
                    coordinates.set(position, point.getCoordinate());
                }
                return GEOMETRY_FACTORY.createLineString(coordinates.toArray(new Coordinate[0]));
            }
        }
        return null;
    }

    public static Geometry lineFromMultiPoint(Geometry geometry) {
        if(!(geometry instanceof MultiPoint)) {
            return null;
        }
        List<Coordinate> coordinates = new ArrayList<>();
        for(Coordinate c : geometry.getCoordinates()){
            coordinates.add(c);
        }
        return GEOMETRY_FACTORY.createLineString(coordinates.toArray(new Coordinate[0]));
    }

    public static Geometry concaveHull(Geometry geometry, double pctConvex, boolean allowHoles){
        ConcaveHull concave_hull = new ConcaveHull(geometry);
        concave_hull.setMaximumEdgeLengthRatio(pctConvex);
        concave_hull.setHolesAllowed(allowHoles);

        return concave_hull.getHull();
    }

    public static Geometry convexHull(Geometry geometry) {
        return geometry.convexHull();
    }

    public static Geometry getCentroid(Geometry geometry) {
        return geometry.getCentroid();
    }

    public static Geometry intersection(Geometry leftGeometry, Geometry rightGeometry) {
        boolean isIntersects = leftGeometry.intersects(rightGeometry);
        if (!isIntersects) {
            return EMPTY_POLYGON;
        }
        if (leftGeometry.contains(rightGeometry)) {
            return rightGeometry;
        }
        if (rightGeometry.contains(leftGeometry)) {
            return leftGeometry;
        }
        return leftGeometry.intersection(rightGeometry);
    }

    public static Geometry makeValid(Geometry geometry, boolean keepCollapsed) {
        GeometryFixer fixer = new GeometryFixer(geometry);
        fixer.setKeepCollapsed(keepCollapsed);
        return fixer.getResult();
    }

    public static Geometry reducePrecision(Geometry geometry, int precisionScale) {
        GeometryPrecisionReducer precisionReduce = new GeometryPrecisionReducer(new PrecisionModel(Math.pow(10, precisionScale)));
        return precisionReduce.reduce(geometry);
    }

    public static Geometry lineMerge(Geometry geometry) {
        if (geometry instanceof MultiLineString) {
            MultiLineString multiLineString = (MultiLineString) geometry;
            int numLineStrings = multiLineString.getNumGeometries();
            LineMerger merger = new LineMerger();
            for (int k = 0; k < numLineStrings; k++) {
                LineString line = (LineString) multiLineString.getGeometryN(k);
                merger.add(line);
            }
            if (merger.getMergedLineStrings().size() == 1) {
                // If the merger was able to join the lines, there will be only one element
                return (Geometry) merger.getMergedLineStrings().iterator().next();
            } else {
                // if the merger couldn't join the lines, it will contain the individual lines, so return the input
                return geometry;
            }
        }
        return EMPTY_GEOMETRY_COLLECTION;
    }

    public static Geometry minimumBoundingCircle(Geometry geometry, int quadrantSegments) {
        MinimumBoundingCircle minimumBoundingCircle = new MinimumBoundingCircle(geometry);
        Coordinate centre = minimumBoundingCircle.getCentre();
        double radius = minimumBoundingCircle.getRadius();
        Geometry circle = null;
        if (centre == null) {
            circle = geometry.getFactory().createPolygon();
        } else {
            circle = geometry.getFactory().createPoint(centre);
            if (radius != 0D) {
                circle = circle.buffer(radius, quadrantSegments);
            }
        }
        return circle;
    }

    public static Geometry lineSubString(Geometry geom, double fromFraction, double toFraction) {
        double length = geom.getLength();
        LengthIndexedLine indexedLine = new LengthIndexedLine(geom);
        Geometry subLine = indexedLine.extractLine(length * fromFraction, length * toFraction);
        return subLine;
    }

    public static Geometry lineInterpolatePoint(Geometry geom, double fraction) {
        double length = geom.getLength();
        LengthIndexedLine indexedLine = new LengthIndexedLine(geom);
        Coordinate interPoint = indexedLine.extractPoint(length * fraction);
        return GEOMETRY_FACTORY.createPoint(interPoint);
    }

    public static Geometry difference(Geometry leftGeometry, Geometry rightGeometry) {
        boolean isIntersects = leftGeometry.intersects(rightGeometry);
        if (!isIntersects) {
            return leftGeometry;
        } else if (rightGeometry.contains(leftGeometry)) {
            return EMPTY_POLYGON;
        } else {
            return leftGeometry.difference(rightGeometry);
        }
    }

    public static Geometry split(Geometry input, Geometry blade) {
        // check input geometry
        return new GeometrySplitter(GEOMETRY_FACTORY).split(input, blade);
    }


    /**
     * get the coordinates of a geometry and transform to Google s2 cell id
     * @param input Geometry
     * @param level integer, minimum level of cells covering the geometry
     * @return List of coordinates
     */
    public static Long[] s2CellIDs(Geometry input, int level) {
        HashSet<S2CellId> cellIds = new HashSet<>();
        List<Geometry> geoms = GeomUtils.extractGeometryCollection(input);
        for (Geometry geom : geoms) {
            try {
                cellIds.addAll(S2Utils.s2RegionToCellIDs(S2Utils.toS2Region(geom), 1, level, Integer.MAX_VALUE - 1));
            } catch (IllegalArgumentException e) {
                // the geometry can't be cast to region, we cover its coordinates, for example, Point
                cellIds.addAll(Arrays.stream(geom.getCoordinates()).map(c -> S2Utils.coordinateToCellID(c, level)).collect(Collectors.toList()));
            }
        }
        return S2Utils.roundCellsToSameLevel(new ArrayList<>(cellIds), level).stream().map(S2CellId::id).collect(Collectors.toList()).toArray(new Long[cellIds.size()]);
    }
}
