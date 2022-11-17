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
}