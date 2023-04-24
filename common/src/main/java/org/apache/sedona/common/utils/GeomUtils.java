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
package org.apache.sedona.common.utils;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.operation.polygonize.Polygonizer;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import java.nio.ByteOrder;
import java.util.*;

import static org.locationtech.jts.geom.Coordinate.NULL_ORDINATE;

public class GeomUtils {
    public static String printGeom(Geometry geom) {
        if(geom.getUserData()!=null) return geom.toText() + "\t" + geom.getUserData();
        else return geom.toText();
    }

    public static String printGeom(Object geom) {
        Geometry g = (Geometry) geom;
        return printGeom(g);
    }

    public static int hashCode(Geometry geom) {
        return geom.getUserData()==null? geom.hashCode():geom.hashCode()*31 + geom.getUserData().hashCode();
    }
    public static boolean equalsTopoGeom(Geometry geom1, Geometry geom2) {
        if (Objects.equals(geom1.getUserData(), geom2.getUserData())) return geom1.equals(geom2);
        return false;
    }

    public static boolean equalsExactGeom(Geometry geom1, Object geom2) {
        if (! (geom2 instanceof Geometry)) return false;
        Geometry g = (Geometry) geom2;
        if (Objects.equals(geom1.getUserData(), g.getUserData())) return geom1.equalsExact(g);
        else return false;
    }

    /**
     * This is for verifying the correctness of two geometries loaded from geojson
     * @param geom1
     * @param geom2
     * @return
     */
    public static boolean equalsExactGeomUnsortedUserData(Geometry geom1, Object geom2) {
        if (! (geom2 instanceof Geometry)) return false;
        Geometry g = (Geometry) geom2;
        if (equalsUserData(geom1.getUserData(), g.getUserData())) return geom1.equalsExact(g);
        else return false;
    }

    /**
     * Use for check if two user data attributes are equal
     * This is mainly used for GeoJSON parser as the column order is uncertain each time
     * @param userData1
     * @param userData2
     * @return
     */
    public static boolean equalsUserData(Object userData1, Object userData2) {
        String[] split1 = ((String) userData1).split("\t");
        String[] split2 = ((String) userData2).split("\t");
        Arrays.sort(split1);
        Arrays.sort(split2);
        return Arrays.equals(split1, split2);
    }

    /**
     * Swaps the XY coordinates of a geometry.
     */
    public static void flipCoordinates(Geometry g) {
        g.apply(new CoordinateSequenceFilter() {

            @Override
            public void filter(CoordinateSequence seq, int i) {
                double oldX = seq.getCoordinate(i).x;
                double oldY = seq.getCoordinateCopy(i).y;
                seq.getCoordinate(i).setX(oldY);
                seq.getCoordinate(i).setY(oldX);
            }

            @Override
            public boolean isGeometryChanged() {
                return true;
            }

            @Override
            public boolean isDone() {
                return false;
            }
        });
    }
    /*
     * Returns a POINT that is guaranteed to lie on the surface.
     */
    public static Geometry getInteriorPoint(Geometry geometry) {
        if(geometry==null) {
            return null;
        }
        return geometry.getInteriorPoint();
    }

    /**
     * Return the nth point from the given geometry (which could be a linestring or a circular linestring)
     * If the value of n is negative, return a point backwards
     * E.g. if n = 1, return 1st point, if n = -1, return last point
     *
     * @param lineString from which the nth point is to be returned
     * @param n is the position of the point in the geometry
     * @return a point
     */
    public static Geometry getNthPoint(LineString lineString, int n) {
        if (lineString == null || n == 0) {
            return null;
        }

        int p = lineString.getNumPoints();
        if (Math.abs(n) > p) {
            return null;
        }

        Coordinate[] nthCoordinate = new Coordinate[1];
        if (n > 0) {
            nthCoordinate[0] = lineString.getCoordinates()[n - 1];
        } else {
            nthCoordinate[0] = lineString.getCoordinates()[p + n];
        }
        return new Point(new CoordinateArraySequence(nthCoordinate), lineString.getFactory());
    }

    public static Geometry getExteriorRing(Geometry geometry) {
        try {
            Polygon polygon = (Polygon) geometry;
            return polygon.getExteriorRing();
        } catch(ClassCastException e) {
            return null;
        }
    }

    public static String getEWKT(Geometry geometry) {
        if(geometry==null) {
            return null;
        }

        int srid = geometry.getSRID();
        String sridString = "";
        if (srid != 0) {
            sridString = "SRID=" + String.valueOf(srid) + ";";
        }

        return sridString + new WKTWriter(GeomUtils.getDimension(geometry)).write(geometry);
    }

    public static String getWKT(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        return new WKTWriter(GeomUtils.getDimension(geometry)).write(geometry);
    }

    public static byte[] getEWKB(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        int endian = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN ? ByteOrderValues.BIG_ENDIAN : ByteOrderValues.LITTLE_ENDIAN;
        WKBWriter writer = new WKBWriter(GeomUtils.getDimension(geometry), endian, geometry.getSRID() != 0);
        return writer.write(geometry);
    }

    public static byte[] getWKB(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        int endian = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN ? ByteOrderValues.BIG_ENDIAN : ByteOrderValues.LITTLE_ENDIAN;
        WKBWriter writer = new WKBWriter(GeomUtils.getDimension(geometry), endian, false);
        return writer.write(geometry);
    }

    public static Geometry get2dGeom(Geometry geom) {
        Coordinate[] coordinates = geom.getCoordinates();
        GeometryFactory geometryFactory = new GeometryFactory();
        CoordinateSequence sequence = geometryFactory.getCoordinateSequenceFactory().create(coordinates);
        if(sequence.getDimension() > 2) {
            for (int i = 0; i < coordinates.length; i++) {
                sequence.setOrdinate(i, 2, NULL_ORDINATE);
            }
            if(sequence.getDimension() == 4) {
                for (int i = 0; i < coordinates.length; i++) {
                    sequence.setOrdinate(i, 3, NULL_ORDINATE);
                }
            }
        }
        geom.geometryChanged();
        return geom;
    }

    public static Geometry buildArea(Geometry geom) {
        if (geom == null || geom.isEmpty()) {
            return geom;
        }
        Polygonizer polygonizer = new Polygonizer();
        polygonizer.add(geom);
        List<Polygon> polygons = (List<Polygon>) polygonizer.getPolygons();
        if (polygons.isEmpty()) {
            return null;
        } else if (polygons.size() == 1) {
            return polygons.get(0);
        }
        int srid = geom.getSRID();
        Map<Polygon, Polygon> parentMap = findFaceHoles(polygons);
        List<Polygon> facesWithEvenAncestors = new ArrayList<>();
        for (Polygon face : polygons) {
            face.normalize();
            if (countParents(parentMap, face) % 2 == 0) {
                facesWithEvenAncestors.add(face);
            }
        }
        UnaryUnionOp unaryUnionOp = new UnaryUnionOp(facesWithEvenAncestors);
        Geometry outputGeom = unaryUnionOp.union();
        if (outputGeom != null) {
            outputGeom.normalize();
            outputGeom.setSRID(srid);
        }
        return outputGeom;
    }

    public static int getDimension(Geometry geometry) {
        return geometry.getCoordinate() != null && !java.lang.Double.isNaN(geometry.getCoordinate().getZ()) ? 3 : 2;
    }

    /**
     * Checks if the geometry only contains geometry of
     * the same dimension. By dimension this refers to whether the
     * geometries are all, for example, lines (1D).
     *
     * @param  geometry geometry to check
     * @return          true iff geometry is homogeneous
     */
    public static boolean geometryIsHomogeneous(Geometry geometry) {
        int dimension = geometry.getDimension();

        if (!geometry.isEmpty()) {
            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                if (dimension != geometry.getGeometryN(i).getDimension()) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Checks if either the geometry is, or contains, only point geometry.
     * GeometryCollections that only contain points will return true.
     *
     * @param  geometry geometry to check
     * @return          true iff geometry is puntal
     */
    public static boolean geometryIsPuntal(Geometry geometry) {
        if (geometry instanceof Puntal) {
            return true;
        } else if (geometryIsHomogeneous(geometry) && geometry.getDimension() == 0) {
            return true;
        }
        return false;
    }

    /**
     * Checks if either the geometry is, or contains, only line geometry.
     * GeometryCollections that only contain lines will return true.
     *
     * @param  geometry geometry to check
     * @return          true iff geometry is lineal
     */
    public static boolean geometryIsLineal(Geometry geometry) {
        if (geometry instanceof Lineal) {
            return true;
        } else if (geometryIsHomogeneous(geometry) && geometry.getDimension() == 1) {
            return true;
        }
        return false;
    }

    /**
     * Checks if either the geometry is, or contains, only polygon geometry.
     * GeometryCollections that only contain polygons will return true.
     *
     * @param  geometry geometry to check
     * @return          true iff geometry is polygonal
     */
    public static boolean geometryIsPolygonal(Geometry geometry) {
        if (geometry instanceof Polygonal) {
            return true;
        } else if (geometryIsHomogeneous(geometry) && geometry.getDimension() == 2) {
            return true;
        }
        return false;
    }

    /**
     * Checks if the geoemetry pair - <code>left</code> and <code>right</code> - should be handled be the current partition - <code>extent</code>.
     *
     * @param left
     * @param right
     * @param extent
     * @return
     */
    public static boolean isDuplicate(Geometry left, Geometry right, HalfOpenRectangle extent) {
        // Handle easy case: points. Since each point is assigned to exactly one partition,
        // different partitions cannot emit duplicate results.
        if (left instanceof Point || right instanceof Point) {
            return false;
        }

        // Neither geometry is a point

        // Check if reference point of the intersection of the bounding boxes lies within
        // the extent of this partition. If not, don't run any checks. Let the partition
        // that contains the reference point do all the work.
        Envelope intersection =
                left.getEnvelopeInternal().intersection(right.getEnvelopeInternal());
        if (!intersection.isNull()) {
            final Point referencePoint =
                    left.getFactory().createPoint(new Coordinate(intersection.getMinX(), intersection.getMinY()));
            if (!extent.contains(referencePoint)) {
                return true;
            }
        }

        return false;
    }

    private static Map<Polygon, Polygon> findFaceHoles(List<Polygon> faces) {
        Map<Polygon, Polygon> parentMap = new HashMap<>();
        faces.sort(Comparator.comparing((Polygon p) -> p.getEnvelope().getArea()).reversed());
        for (int i = 0; i < faces.size(); i++) {
            Polygon face = faces.get(i);
            int nHoles = face.getNumInteriorRing();
            for (int h = 0; h < nHoles; h++) {
                Geometry hole = face.getInteriorRingN(h);
                for (int j = i + 1; j < faces.size(); j++) {
                    Polygon face2 = faces.get(j);
                    if (parentMap.containsKey(face2)) {
                        continue;
                    }
                    Geometry face2ExteriorRing = face2.getExteriorRing();
                    if (face2ExteriorRing.equals(hole)) {
                        parentMap.put(face2, face);
                    }
                }
            }
        }
        return parentMap;
    }

    private static int countParents(Map<Polygon, Polygon> parentMap, Polygon face) {
        int pCount = 0;
        while (parentMap.containsKey(face)) {
            pCount++;
            face = parentMap.get(face);
        }
        return pCount;
    }

    public static <T extends Geometry> List<Geometry> extractGeometryCollection(Geometry geom, Class<T> geomType){
        ArrayList<Geometry> leafs = new ArrayList<>();
        if (!(geom instanceof GeometryCollection)) {
            if (geomType.isAssignableFrom(geom.getClass())) {
                leafs.add(geom);
            }
            return leafs;
        }
        LinkedList<GeometryCollection> parents = new LinkedList<>();
        parents.add((GeometryCollection) geom);
        while (!parents.isEmpty()) {
            GeometryCollection parent = parents.removeFirst();
            for (int i = 0;i < parent.getNumGeometries(); i++) {
                Geometry child = parent.getGeometryN(i);
                if (child instanceof GeometryCollection) {
                    parents.add((GeometryCollection) child);
                } else {
                    if (geomType.isAssignableFrom(child.getClass())) {
                        leafs.add(child);
                    }
                }
            }
        }
        return leafs;
    }

    public static List<Geometry> extractGeometryCollection(Geometry geom){
        return extractGeometryCollection(geom, Geometry.class);
    }

    public static Geometry[] getSubGeometries(Geometry geom) {
        Geometry[] geometries = new Geometry[geom.getNumGeometries()];
        for ( int i = 0; i < geom.getNumGeometries() ; i++) {
            geometries[i] = geom.getGeometryN(i);
        }
        return geometries;
    }
}
