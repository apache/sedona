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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.subDivide.GeometrySubDivider;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.GeometryGeoHashEncoder;
import org.apache.sedona.common.utils.GeometrySplitter;
import org.apache.sedona.common.utils.S2Utils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.algorithm.MinimumBoundingCircle;
import org.locationtech.jts.algorithm.hull.ConcaveHull;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.locationtech.jts.io.kml.KMLWriter;
import org.locationtech.jts.linearref.LengthIndexedLine;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.locationtech.jts.operation.distance3d.Distance3DOp;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.operation.valid.IsSimpleOp;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;
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
import java.util.stream.Collectors;

import static com.google.common.geometry.S2.DBL_EPSILON;


public class Functions {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static Geometry EMPTY_POLYGON = GEOMETRY_FACTORY.createPolygon(null, null);
    private static GeometryCollection EMPTY_GEOMETRY_COLLECTION = GEOMETRY_FACTORY.createGeometryCollection(null);
    private static final double DEFAULT_TOLERANCE = 1e-6;
    private static final int DEFAULT_MAX_ITER = 1000;

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

    public static Geometry transform(Geometry geometry, String targetCRS)
            throws FactoryException, TransformException {
        return transform(geometry, null, targetCRS, true);
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS)
        throws FactoryException, TransformException {
        return transform(geometry, sourceCRS, targetCRS, true);
    }

    public static Geometry transform(Geometry geometry, String sourceCRScode, String targetCRScode, boolean lenient)
        throws FactoryException, TransformException {
        CoordinateReferenceSystem targetCRS = parseCRSString(targetCRScode);
        return transformToGivenTarget(geometry, sourceCRScode, targetCRS, lenient);
    }

    /**
     * Transform a geometry from one CRS to another. If sourceCRS is not specified, it will be
     * extracted from the geometry. If lenient is true, the transformation will be lenient.
     * This function is used by the implicit CRS transformation in Sedona rasters.
     * @param geometry
     * @param sourceCRScode
     * @param targetCRS
     * @param lenient
     * @return
     * @throws FactoryException
     * @throws TransformException
     */
    public static Geometry transformToGivenTarget(Geometry geometry, String sourceCRScode, CoordinateReferenceSystem targetCRS, boolean lenient)
            throws FactoryException, TransformException
    {
        // If sourceCRS is not specified, try to get it from the geometry
        if (sourceCRScode == null) {
            int srid = geometry.getSRID();
            if (srid != 0) {
                sourceCRScode = "epsg:" + srid;
            }
            else {
                // If SRID is not set, throw an exception
                throw new IllegalArgumentException("Source CRS must be specified. No SRID found on geometry.");
            }
        }
        CoordinateReferenceSystem sourceCRS = parseCRSString(sourceCRScode);
        // If sourceCRS and targetCRS are equal, return the geometry unchanged
        if (!CRS.equalsIgnoreMetadata(sourceCRS, targetCRS)) {
            MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
            return JTS.transform(geometry, transform);
        }
        else return geometry;
    }

    private static CoordinateReferenceSystem parseCRSString(String CRSString)
            throws FactoryException
    {
        try {
            // Try to parse as a well-known CRS code
            // Longitude first, then latitude
            return CRS.decode(CRSString, true);
        }
        catch (NoSuchAuthorityCodeException e) {
            try {
                // Try to parse as a WKT CRS string, longitude first
                Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
                return ReferencingFactoryFinder.getCRSFactory(hints).createFromWKT(CRSString);
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

    public static Geometry closestPoint(Geometry left, Geometry right) {
        DistanceOp distanceOp = new DistanceOp(left, right);
        try {
            Coordinate[] closestPoints = distanceOp.nearestPoints();
            return GEOMETRY_FACTORY.createPoint(closestPoints[0]);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("ST_ClosestPoint doesn't support empty geometry object.");
        }
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

    public static Pair<Geometry, Double> minimumBoundingRadius(Geometry geometry) {
        MinimumBoundingCircle minimumBoundingCircle = new MinimumBoundingCircle(geometry);
        Coordinate coods = minimumBoundingCircle.getCentre();
        double radius = minimumBoundingCircle.getRadius();
        Point centre = GEOMETRY_FACTORY.createPoint(coods);
        return Pair.of(centre, radius);
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

    public static Integer dimension(Geometry geometry) {
        Integer dimension = geometry.getDimension();
        // unknown dimension such as an empty GEOMETRYCOLLECTION
        if (dimension < 0) {
            dimension = 0;
        }
        return dimension;
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


    // create static function named simplifyPreserveTopology
    public static Geometry simplifyPreserveTopology(Geometry geometry, double distanceTolerance) {
        return TopologyPreservingSimplifier.simplify(geometry, distanceTolerance);
    }

    public static String geometryType(Geometry geometry) {
        return "ST_" + geometry.getGeometryType();
    }

    public static String geometryTypeWithMeasured(Geometry geometry) {
        String geometryType = geometry.getGeometryType().toUpperCase();
        if (GeomUtils.isMeasuredGeometry(geometry)) {
            geometryType += "M";
        }
        return geometryType;
    }

    public static Geometry startPoint(Geometry geometry) {
        if (geometry instanceof LineString) {
            LineString line = (LineString) geometry;
            return line.getStartPoint();
        }
        return null;
    }

    public static Geometry endPoint(Geometry geometry) {
        if (geometry instanceof LineString) {
            LineString line = (LineString) geometry;
            return line.getEndPoint();
        }
        return null;
    }

    public static Geometry[] dump(Geometry geometry) {
        int numGeom = geometry.getNumGeometries();
        if (geometry instanceof GeometryCollection) {
            Geometry[] geoms = new Geometry[geometry.getNumGeometries()];
            for (int i = 0; i < numGeom; i++) {
                geoms[i] = geometry.getGeometryN(i);
            }
            return geoms;
        } else {
            return new Geometry[] {geometry};
        }
    }

    public static Geometry[] dumpPoints(Geometry geometry) {
        return Arrays.stream(geometry.getCoordinates()).map(GEOMETRY_FACTORY::createPoint).toArray(Point[]::new);
    }

    public static Geometry symDifference(Geometry leftGeom, Geometry rightGeom) {
        return leftGeom.symDifference(rightGeom);
    }

    public static Geometry union(Geometry leftGeom, Geometry rightGeom) {
        return leftGeom.union(rightGeom);
    }

    public static Geometry createMultiGeometryFromOneElement(Geometry geometry) {
        if (geometry instanceof Circle) {
            return GEOMETRY_FACTORY.createGeometryCollection(new Circle[] {(Circle) geometry});
        } else if (geometry instanceof GeometryCollection) {
            return geometry;
        } else if (geometry instanceof  LineString) {
            return GEOMETRY_FACTORY.createMultiLineString(new LineString[]{(LineString) geometry});
        } else if (geometry instanceof Point) {
            return GEOMETRY_FACTORY.createMultiPoint(new Point[] {(Point) geometry});
        } else if (geometry instanceof Polygon) {
            return GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {(Polygon) geometry});
        } else {
            return GEOMETRY_FACTORY.createGeometryCollection();
        }
    }

    public static Geometry[] subDivide(Geometry geometry, int maxVertices) {
        return GeometrySubDivider.subDivide(geometry, maxVertices);
    }

    public static Geometry makeLine(Geometry geom1, Geometry geom2) {
       Geometry[] geoms = new Geometry[]{geom1, geom2};
       return makeLine(geoms);
    }

    public static Geometry makeLine(Geometry[] geoms) {
        ArrayList<Coordinate> coordinates = new ArrayList<>();      
        for (Geometry geom : geoms) {
            if (geom instanceof Point || geom instanceof MultiPoint || geom instanceof LineString) {
                for (Coordinate coord : geom.getCoordinates()) {
                    coordinates.add(coord);
                }
            }
            else {
                throw new IllegalArgumentException("ST_MakeLine only supports Point, MultiPoint and LineString geometries");
            }
        }
        
        Coordinate[] coords = coordinates.toArray(new Coordinate[0]);
        return GEOMETRY_FACTORY.createLineString(coords);
    }

    public static Geometry makePolygon(Geometry shell, Geometry[] holes) {
        try {
            if (holes != null) {
                LinearRing[] interiorRings =  Arrays.stream(holes).filter(
                        h -> h != null && !h.isEmpty() && h instanceof LineString && ((LineString) h).isClosed()
                ).map(
                        h -> GEOMETRY_FACTORY.createLinearRing(h.getCoordinates())
                ).toArray(LinearRing[]::new);
                if (interiorRings.length != 0) {
                    return GEOMETRY_FACTORY.createPolygon(
                            GEOMETRY_FACTORY.createLinearRing(shell.getCoordinates()),
                            Arrays.stream(holes).filter(
                                    h -> h != null && !h.isEmpty() && h instanceof LineString && ((LineString) h).isClosed()
                            ).map(
                                    h -> GEOMETRY_FACTORY.createLinearRing(h.getCoordinates())
                            ).toArray(LinearRing[]::new)
                    );
                }
            }
            return GEOMETRY_FACTORY.createPolygon(
                    GEOMETRY_FACTORY.createLinearRing(shell.getCoordinates())
            );
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public static Geometry makepolygonWithSRID(Geometry lineString, Integer srid) {
        Geometry geom = makePolygon(lineString, null);
        if(geom != null) {
            geom.setSRID(srid);
        }
        return geom;
    }
    
    public static Geometry createMultiGeometry(Geometry[] geometries) {
        if (geometries.length > 1){
            return GEOMETRY_FACTORY.buildGeometry(Arrays.asList(geometries));
        }
        else if(geometries.length==1){
            return createMultiGeometryFromOneElement(geometries[0]);
        }
        else{
            return GEOMETRY_FACTORY.createGeometryCollection();
        }
    }

    public static Geometry collectionExtract(Geometry geometry, Integer geomType) {
        if (geomType == null) {
            return collectionExtract(geometry);
        }
        Class<? extends Geometry> geomClass;
        GeometryCollection emptyResult;
        switch (geomType) {
            case 1:
                geomClass = Point.class;
                emptyResult = GEOMETRY_FACTORY.createMultiPoint();
                break;
            case 2:
                geomClass = LineString.class;
                emptyResult = GEOMETRY_FACTORY.createMultiLineString();
                break;
            case 3:
                geomClass = Polygon.class;
                emptyResult = GEOMETRY_FACTORY.createMultiPolygon();
                break;
            default:
                throw new IllegalArgumentException("Invalid geometry type");
        }
        List<Geometry> geometries = GeomUtils.extractGeometryCollection(geometry, geomClass);
        if (geometries.isEmpty()) {
            return emptyResult;
        }
        return Functions.createMultiGeometry(geometries.toArray(new Geometry[0]));
    }

    public static Geometry collectionExtract(Geometry geometry) {
        List<Geometry> geometries = GeomUtils.extractGeometryCollection(geometry);
        Polygon[] polygons = geometries.stream().filter(g -> g instanceof Polygon).toArray(Polygon[]::new);
        if (polygons.length > 0) {
            return GEOMETRY_FACTORY.createMultiPolygon(polygons);
        }
        LineString[] lines = geometries.stream().filter(g -> g instanceof LineString).toArray(LineString[]::new);
        if (lines.length > 0) {
            return GEOMETRY_FACTORY.createMultiLineString(lines);
        }
        Point[] points = geometries.stream().filter(g -> g instanceof Point).toArray(Point[]::new);
        if (points.length > 0) {
            return GEOMETRY_FACTORY.createMultiPoint(points);
        }
        return GEOMETRY_FACTORY.createGeometryCollection();
    }


    // ported from https://github.com/postgis/postgis/blob/f6ed58d1fdc865d55d348212d02c11a10aeb2b30/liblwgeom/lwgeom_median.c
    // geometry ST_GeometricMedian ( geometry g , float8 tolerance , int max_iter , boolean fail_if_not_converged );

    private static double distance3d(Coordinate p1, Coordinate p2) {
        double dx = p2.x - p1.x;
        double dy = p2.y - p1.y;
        double dz = p2.z - p1.z;
        return Math.sqrt(dx * dx + dy * dy + dz * dz);
    }

    private static void distances(Coordinate curr, Coordinate[] points, double[] distances) {
        for(int i = 0; i < points.length; i++) {
            distances[i] = distance3d(curr, points[i]);
        }
    }

    private static double iteratePoints(Coordinate curr, Coordinate[] points, double[] distances) {
        Coordinate next = new Coordinate(0, 0, 0);
        double delta = 0;
        double denom = 0;
        boolean hit = false;
        distances(curr, points, distances);

        for (int i = 0; i < points.length; i++) {
            /* we need to use lower epsilon than in FP_IS_ZERO in the loop for calculation to converge */
            double distance = distances[i];
            if (distance > DBL_EPSILON) {
                Coordinate coordinate = points[i];
                next.x += coordinate.x / distance;
                next.y += coordinate.y / distance;
                next.z += coordinate.z / distance;
                denom += 1.0 / distance;
            } else {
                hit = true;
            }
        }
        /* negative weight shouldn't get here */
        //assert(denom >= 0);

        /* denom is zero in case of multipoint of single point when we've converged perfectly */
        if (denom > DBL_EPSILON) {
            next.x /= denom;
            next.y /= denom;
            next.z /= denom;

            /* If any of the intermediate points in the calculation is found in the
             * set of input points, the standard Weiszfeld method gets stuck with a
             * divide-by-zero.
             *
             * To get ourselves out of the hole, we follow an alternate procedure to
             * get the next iteration, as described in:
             *
             * Vardi, Y. and Zhang, C. (2011) "A modified Weiszfeld algorithm for the
             * Fermat-Weber location problem."  Math. Program., Ser. A 90: 559-566.
             * DOI 10.1007/s101070100222
             *
             * Available online at the time of this writing at
             * http://www.stat.rutgers.edu/home/cunhui/papers/43.pdf
             */
            if (hit) {
                double dx = 0;
                double dy = 0;
                double dz = 0;
                for (int i = 0; i < points.length; i++) {
                    double distance = distances[i];
                    if (distance > DBL_EPSILON) {
                        Coordinate coordinate = points[i];
                        dx += (coordinate.x - curr.x) / distance;
                        dy += (coordinate.y - curr.y) / distance;
                        dz += (coordinate.z - curr.z) / distance;
                    }
                }
                double dSqr = Math.sqrt(dx*dx + dy*dy + dz*dz);
                /* Avoid division by zero if the intermediate point is the median */
                if (dSqr > DBL_EPSILON) {
                    double rInv = Math.max(0, 1.0 / dSqr);
                    next.x = (1.0 - rInv)*next.x + rInv*curr.x;
                    next.y = (1.0 - rInv)*next.y + rInv*curr.y;
                    next.z = (1.0 - rInv)*next.z + rInv*curr.z;
                }
            }
            delta = distance3d(curr, next);
            curr.x = next.x;
            curr.y = next.y;
            curr.z = next.z;
        }
        return delta;
    }

    private static Coordinate initGuess(Coordinate[] points) {
        Coordinate guess = new Coordinate(0, 0, 0);
        for (Coordinate point : points) {
            guess.x += point.x / points.length;
            guess.y += point.y / points.length;
            guess.z += point.z / points.length;
        }
        return guess;
    }

    private static Coordinate[] extractCoordinates(Geometry geometry) {
        Coordinate[] points = geometry.getCoordinates();
        if(points.length == 0)
            return points;

        Coordinate[] coordinates = new Coordinate[points.length];
        for(int i = 0; i < points.length; i++) {
            boolean is3d = !Double.isNaN(points[i].z);
            coordinates[i] = points[i].copy();
            if(!is3d)
                coordinates[i].z = 0.0;
        }
        return coordinates;
    }

    public static int numPoints(Geometry geometry) throws Exception {
        String geometryType = geometry.getGeometryType();
        if (!(Geometry.TYPENAME_LINESTRING.equalsIgnoreCase(geometryType))) {
            throw new IllegalArgumentException("Unsupported geometry type: " + geometryType + ", only LineString geometry is supported.");
        }
        return geometry.getNumPoints();
    }

    public static Geometry force3D(Geometry geometry, double zValue) {
        return GeomUtils.get3DGeom(geometry, zValue);
    }

    public static Geometry force3D(Geometry geometry) {
       return GeomUtils.get3DGeom(geometry, 0.0);
    }

    public static Integer nRings(Geometry geometry) throws Exception {
        String geometryType = geometry.getGeometryType();
        if (!(geometry instanceof Polygon || geometry instanceof MultiPolygon)) {
            throw new IllegalArgumentException("Unsupported geometry type: " + geometryType + ", only Polygon or MultiPolygon geometries are supported.");
        }
        int numRings = 0;
        if (geometry instanceof Polygon) {
            Polygon polygon = (Polygon) geometry;
            numRings = GeomUtils.getPolygonNumRings(polygon);
        }else {
            MultiPolygon multiPolygon = (MultiPolygon) geometry;
            int numPolygons = multiPolygon.getNumGeometries();
            for (int i = 0; i < numPolygons; i++) {
                Polygon polygon = (Polygon) multiPolygon.getGeometryN(i);
                numRings += GeomUtils.getPolygonNumRings(polygon);
            }
        }
        return numRings;
    }

    public static Geometry translate(Geometry geometry, double deltaX, double deltaY, double deltaZ) {
        if (!geometry.isEmpty()) {
            GeomUtils.translateGeom(geometry, deltaX, deltaY, deltaZ);
        }
        return geometry;
    }

    public static Geometry translate(Geometry geometry, double deltaX, double deltaY) {
        if (!geometry.isEmpty()) {
            GeomUtils.translateGeom(geometry, deltaX, deltaY, 0.0);
        }
        return geometry;
    }

    public static Geometry affine(Geometry geometry, double a, double b, double c, double d, double e, double f, double g, double h, double i, double xOff, double yOff,
                                  double zOff) {
        if (!geometry.isEmpty()) {
            GeomUtils.affineGeom(geometry, a, b, c, d, e, f, g, h, i, xOff, yOff, zOff);
        }
        return geometry;
    }

    public static Geometry affine(Geometry geometry, double a, double b, double d, double e, double xOff, double yOff) {
        if (!geometry.isEmpty()) {
            GeomUtils.affineGeom(geometry, a, b, null, d, e, null, null, null, null, xOff, yOff, null);
        }
        return geometry;
    }

    public static Geometry geometricMedian(Geometry geometry, double tolerance, int maxIter, boolean failIfNotConverged) throws Exception {
        String geometryType = geometry.getGeometryType();
        if(!(Geometry.TYPENAME_POINT.equals(geometryType) || Geometry.TYPENAME_MULTIPOINT.equals(geometryType))) {
            throw new Exception("Unsupported geometry type: " + geometryType);
        }
        Coordinate[] coordinates = extractCoordinates(geometry);
        if(coordinates.length == 0)
            return new Point(null, GEOMETRY_FACTORY);
        Coordinate median = initGuess(coordinates);
        double delta = Double.MAX_VALUE;
        double[] distances = new double[coordinates.length]; // preallocate to reduce gc pressure for large iterations
        for(int i = 0; i < maxIter && delta > tolerance; i++)
            delta = iteratePoints(median, coordinates, distances);
        if (failIfNotConverged && delta > tolerance)
            throw new Exception(String.format("Median failed to converge within %.1E after %d iterations.", tolerance, maxIter));
        boolean is3d = !Double.isNaN(geometry.getCoordinate().z);
        if(!is3d)
            median.z = Double.NaN;
        Point point = new Point(new CoordinateArraySequence(new Coordinate[]{median}), GEOMETRY_FACTORY);
        point.setSRID(geometry.getSRID());
        return point;
    }

    public static Geometry geometricMedian(Geometry geometry, double tolerance, int maxIter) throws Exception {
        return geometricMedian(geometry, tolerance, maxIter, false);
    }

    public static Geometry geometricMedian(Geometry geometry, double tolerance) throws Exception {
        return geometricMedian(geometry, tolerance, DEFAULT_MAX_ITER, false);
    }

    public static Geometry geometricMedian(Geometry geometry) throws Exception {
        return geometricMedian(geometry, DEFAULT_TOLERANCE, DEFAULT_MAX_ITER, false);
    }

    public static double frechetDistance(Geometry g1, Geometry g2) {
        return GeomUtils.getFrechetDistance(g1, g2);
    }

    public static boolean isCollection(Geometry geometry) {
        String geoType = geometry.getGeometryType();
        return Geometry.TYPENAME_GEOMETRYCOLLECTION.equalsIgnoreCase(geoType) ||
                Geometry.TYPENAME_MULTIPOINT.equalsIgnoreCase(geoType) ||
                Geometry.TYPENAME_MULTIPOLYGON.equalsIgnoreCase(geoType) ||
                Geometry.TYPENAME_MULTILINESTRING.equalsIgnoreCase(geoType);
    }

    public static Geometry boundingDiagonal(Geometry geometry) {
        if (geometry.isEmpty()) {
            return GEOMETRY_FACTORY.createLineString();
        }else {
            Double startX = null, startY = null, startZ = null,
                    endX = null, endY = null, endZ = null;
            boolean is3d = !Double.isNaN(geometry.getCoordinate().z);
            for (Coordinate currCoordinate : geometry.getCoordinates()) {
                startX = startX == null ? currCoordinate.getX() : Math.min(startX, currCoordinate.getX());
                startY = startY == null ? currCoordinate.getY() : Math.min(startY, currCoordinate.getY());

                endX = endX == null ? currCoordinate.getX() : Math.max(endX, currCoordinate.getX());
                endY = endY == null ? currCoordinate.getY() : Math.max(endY, currCoordinate.getY());
                if (is3d) {
                    Double geomZ = currCoordinate.getZ();
                    startZ = startZ == null ? currCoordinate.getZ() : Math.min(startZ, currCoordinate.getZ());
                    endZ = endZ == null ? currCoordinate.getZ() : Math.max(endZ, currCoordinate.getZ());
                }
            }
            Coordinate startCoordinate;
            Coordinate endCoordinate;
            if (is3d) {
                startCoordinate = new Coordinate(startX, startY, startZ);
                endCoordinate = new Coordinate(endX, endY, endZ);
            }else {
                startCoordinate = new Coordinate(startX, startY);
                endCoordinate = new Coordinate(endX, endY);
            }
            return GEOMETRY_FACTORY.createLineString(new Coordinate[] {startCoordinate, endCoordinate});
        }
    }

    public static double angle(Geometry point1, Geometry point2, Geometry point3, Geometry point4) throws IllegalArgumentException {
        if (point3 == null && point4 == null) return Functions.angle(point1, point2);
        else if (point4 == null) return Functions.angle(point1, point2, point3);
        if (GeomUtils.isAnyGeomEmpty(point1, point2, point3, point4)) throw new IllegalArgumentException("ST_Angle cannot support empty geometries.");
        if (!(point1 instanceof Point && point2 instanceof Point && point3 instanceof Point && point4 instanceof Point)) throw new IllegalArgumentException("ST_Angle supports either only POINT or only LINESTRING geometries.");
        return GeomUtils.calcAngle(point1.getCoordinate(), point2.getCoordinate(), point3.getCoordinate(), point4.getCoordinate());
    }

    public static double angle(Geometry point1, Geometry point2, Geometry point3) throws IllegalArgumentException {
        if (GeomUtils.isAnyGeomEmpty(point1, point2, point3)) throw new IllegalArgumentException("ST_Angle cannot support empty geometries.");
        if (!(point1 instanceof Point && point2 instanceof Point && point3 instanceof Point)) throw new IllegalArgumentException("ST_Angle supports either only POINT or only LINESTRING geometries.");
        return GeomUtils.calcAngle(point2.getCoordinate(), point1.getCoordinate(), point2.getCoordinate(), point3.getCoordinate());
    }

    public static double angle(Geometry line1, Geometry line2) throws IllegalArgumentException {
        if (GeomUtils.isAnyGeomEmpty(line1, line2)) throw new IllegalArgumentException("ST_Angle cannot support empty geometries.");
        if (!(line1 instanceof LineString && line2 instanceof LineString)) throw new IllegalArgumentException("ST_Angle supports either only POINT or only LINESTRING geometries.");
        Coordinate[] startEndLine1 = GeomUtils.getStartEndCoordinates(line1);
        Coordinate[] startEndLine2 = GeomUtils.getStartEndCoordinates(line2);
        assert startEndLine1 != null;
        assert startEndLine2 != null;
        return GeomUtils.calcAngle(startEndLine1[0], startEndLine1[1], startEndLine2[0], startEndLine2[1]);
    }

    public static double degrees(double angleInRadian) {
        return GeomUtils.toDegrees(angleInRadian);
    }

    public static Double hausdorffDistance(Geometry g1, Geometry g2, double densityFrac) throws Exception {
        return GeomUtils.getHausdorffDistance(g1, g2, densityFrac);
    }

    public static Double hausdorffDistance(Geometry g1, Geometry g2) throws Exception{
        return GeomUtils.getHausdorffDistance(g1, g2, -1);
    }

    public static Geometry voronoiPolygons(Geometry geom, double tolerance, Geometry extendTo) {
        if(geom == null) {
            return null;
        }
        VoronoiDiagramBuilder builder = new VoronoiDiagramBuilder();
        builder.setSites(geom);
        builder.setTolerance(tolerance);
        if (extendTo != null) {
            builder.setClipEnvelope(extendTo.getEnvelopeInternal());
        }
        else{
            Envelope e = geom.getEnvelopeInternal();
            e.expandBy(Math.max(e.getWidth(), e.getHeight()));
            builder.setClipEnvelope(e);
        }
        return builder.getDiagram(GEOMETRY_FACTORY);
    }
    
}
