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
import com.google.common.math.DoubleMath;
import com.sun.org.apache.xpath.internal.operations.Mult;
import org.apache.sedona.common.sphere.Haversine;
import org.apache.sedona.common.sphere.Spheroid;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.S2Utils;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import javax.sound.sampled.Line;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class FunctionsTest {
    public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    protected static final double FP_TOLERANCE = 1e-12;
    protected static final CoordinateSequenceComparator COORDINATE_SEQUENCE_COMPARATOR = new CoordinateSequenceComparator(2){
        @Override
        protected int compareCoordinate(CoordinateSequence s1, CoordinateSequence s2, int i, int dimension) {
            for (int d = 0; d < dimension; d++) {
                double ord1 = s1.getOrdinate(i, d);
                double ord2 = s2.getOrdinate(i, d);
                int comp = DoubleMath.fuzzyCompare(ord1, ord2, FP_TOLERANCE);
                if (comp != 0) return comp;
            }
            return 0;
        }
    };

    private final WKTReader wktReader = new WKTReader();

    private Coordinate[] coordArray(double... coordValues) {
        Coordinate[] coords = new Coordinate[(int)(coordValues.length / 2)];
        for (int i = 0; i < coordValues.length; i += 2) {
            coords[(int)(i / 2)] = new Coordinate(coordValues[i], coordValues[i+1]);
        }
        return coords;
    }

    private Coordinate[] coordArray3d(double... coordValues) {
        Coordinate[] coords = new Coordinate[(int)(coordValues.length / 3)];
        for (int i = 0; i < coordValues.length; i += 3) {
            coords[(int)(i / 3)] = new Coordinate(coordValues[i], coordValues[i+1], coordValues[i+2]);
        }
        return coords;
    }

    @Test
    public void splitLineStringByMultipoint() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0));
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 1.0, 1.0));

        String actualResult = Functions.split(lineString, multiPoint).norm().toText();
        String expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitMultiLineStringByMultiPoint() {
        LineString[] lineStrings = new LineString[]{
            GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0)),
            GEOMETRY_FACTORY.createLineString(coordArray(3.0, 3.0, 4.5, 4.5, 5.0, 5.0))
        };
        MultiLineString multiLineString = GEOMETRY_FACTORY.createMultiLineString(lineStrings);
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 1.0, 1.0, 3.5, 3.5, 4.0, 4.0));

        String actualResult = Functions.split(multiLineString, multiPoint).norm().toText();
        String expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2), (3 3, 3.5 3.5), (3.5 3.5, 4 4), (4 4, 4.5 4.5, 5 5))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitLineStringByMultiPointWithReverseOrder() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0));
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(1.0, 1.0, 0.5, 0.5));

        String actualResult = Functions.split(lineString, multiPoint).norm().toText();
        String expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitLineStringWithReverseOrderByMultiPoint() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(2.0, 2.0, 1.5, 1.5, 0.0, 0.0));
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 1.0, 1.0));

        String actualResult = Functions.split(lineString, multiPoint).norm().toText();
        String expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitLineStringWithVerticalSegmentByMultiPoint() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1.5, 0.0, 1.5, 1.5, 2.0, 2.0));
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(1.5, 0.5, 1.5, 1.0));

        String actualResult = Functions.split(lineString, multiPoint).norm().toText();
        String expectedResult = "MULTILINESTRING ((1.5 0, 1.5 0.5), (1.5 0.5, 1.5 1), (1.5 1, 1.5 1.5, 2 2))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitLineStringByLineString() {
        LineString lineStringInput = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 2.0, 2.0));
        LineString lineStringBlade = GEOMETRY_FACTORY.createLineString(coordArray(1.0, 0.0, 1.0, 3.0));

        String actualResult = Functions.split(lineStringInput, lineStringBlade).norm().toText();
        String expectedResult = "MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitLineStringByPolygon() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 2.0, 2.0));
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1.0, 0.0, 1.0, 3.0, 3.0, 3.0, 3.0, 0.0, 1.0, 0.0));

        String actualResult = Functions.split(lineString, polygon).norm().toText();
        String expectedResult = "MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitPolygonByLineString() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1.0, 1.0, 5.0, 1.0, 5.0, 5.0, 1.0, 5.0, 1.0, 1.0));
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(3.0, 0.0, 3.0, 6.0));

        String actualResult = Functions.split(polygon, lineString).norm().toText();
        String expectedResult = "MULTIPOLYGON (((1 1, 1 5, 3 5, 3 1, 1 1)), ((3 1, 3 5, 5 5, 5 1, 3 1)))";

        assertEquals(actualResult, expectedResult);
    }

    // // overlapping multipolygon by linestring
    @Test
    public void splitOverlappingMultiPolygonByLineString() {
        Polygon[] polygons = new Polygon[]{
            GEOMETRY_FACTORY.createPolygon(coordArray(1.0, 1.0, 5.0, 1.0, 5.0, 5.0, 1.0, 5.0, 1.0, 1.0)),
            GEOMETRY_FACTORY.createPolygon(coordArray(2.0, 1.0, 6.0, 1.0, 6.0, 5.0, 2.0, 5.0, 2.0, 1.0))
        };
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(polygons);
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(3.0, 0.0, 3.0, 6.0));

        String actualResult = Functions.split(multiPolygon, lineString).norm().toText();
        String expectedResult = "MULTIPOLYGON (((1 1, 1 5, 3 5, 3 1, 1 1)), ((2 1, 2 5, 3 5, 3 1, 2 1)), ((3 1, 3 5, 5 5, 5 1, 3 1)), ((3 1, 3 5, 6 5, 6 1, 3 1)))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitPolygonByInsideRing() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1.0, 1.0, 5.0, 1.0, 5.0, 5.0, 1.0, 5.0, 1.0, 1.0));
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(2.0, 2.0, 3.0, 2.0, 3.0, 3.0, 2.0, 3.0, 2.0, 2.0));

        String actualResult = Functions.split(polygon, lineString).norm().toText();
        String expectedResult = "MULTIPOLYGON (((1 1, 1 5, 5 5, 5 1, 1 1), (2 2, 3 2, 3 3, 2 3, 2 2)), ((2 2, 2 3, 3 3, 3 2, 2 2)))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitPolygonByLineOutsideOfPolygon() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1.0, 1.0, 5.0, 1.0, 5.0, 5.0, 1.0, 5.0, 1.0, 1.0));
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(10.0, 10.0, 11.0, 11.0));

        String actualResult = Functions.split(polygon, lineString).norm().toText();
        String expectedResult = "MULTIPOLYGON (((1 1, 1 5, 5 5, 5 1, 1 1)))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitPolygonByPolygon() {
        Polygon polygonInput = GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 4.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0, 0.0));
        Polygon polygonBlade = GEOMETRY_FACTORY.createPolygon(coordArray(2.0, 0.0, 6.0, 0.0, 6.0, 4.0, 2.0, 4.0, 2.0, 0.0));

        String actualResult = Functions.split(polygonInput, polygonBlade).norm().toText();
        String expectedResult = "MULTIPOLYGON (((0 0, 0 4, 2 4, 2 0, 0 0)), ((2 0, 2 4, 4 4, 4 0, 2 0)))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitPolygonWithHoleByLineStringThroughHole() {
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coordArray(0.0, 0.0, 4.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0, 0.0));
        LinearRing[] holes = new LinearRing[]{
            GEOMETRY_FACTORY.createLinearRing(coordArray(1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0))
        };
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, holes);
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1.5, -1.0, 1.5, 5.0));

        String actualResult = Functions.split(polygon, lineString).norm().toText();
        String expectedResult = "MULTIPOLYGON (((0 0, 0 4, 1.5 4, 1.5 2, 1 2, 1 1, 1.5 1, 1.5 0, 0 0)), ((1.5 0, 1.5 1, 2 1, 2 2, 1.5 2, 1.5 4, 4 4, 4 0, 1.5 0)))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitPolygonWithHoleByLineStringNotThroughHole() {
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coordArray(0.0, 0.0, 4.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0, 0.0));
        LinearRing[] holes = new LinearRing[]{
            GEOMETRY_FACTORY.createLinearRing(coordArray(1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0))
        };
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, holes);
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(3.0, -1.0, 3.0, 5.0));

        String actualResult = Functions.split(polygon, lineString).norm().toText();
        String expectedResult = "MULTIPOLYGON (((0 0, 0 4, 3 4, 3 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), ((3 0, 3 4, 4 4, 4 0, 3 0)))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitHomogeneousLinealGeometryCollectionByMultiPoint() {
        LineString[] lineStrings = new LineString[]{
            GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5, 2.0, 2.0)),
            GEOMETRY_FACTORY.createLineString(coordArray(3.0, 3.0, 4.5, 4.5, 5.0, 5.0))
        };
        GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(lineStrings);
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 1.0, 1.0, 3.5, 3.5, 4.0, 4.0));

        String actualResult = Functions.split(geometryCollection, multiPoint).norm().toText();
        String expectedResult = "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2), (3 3, 3.5 3.5), (3.5 3.5, 4 4), (4 4, 4.5 4.5, 5 5))";

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void splitHeterogeneousGeometryCollection() {
        Geometry[] geometry = new Geometry[]{
            GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5)),
            GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0))
        };
        GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(geometry);
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0.5, 0.0, 0.5, 5.0));

        Geometry actualResult = Functions.split(geometryCollection, lineString);

        assertNull(actualResult);
    }

    private static boolean intersects(Set<?> s1, Set<?> s2) {
        Set<?> copy = new HashSet<>(s1);
        copy.retainAll(s2);
        return !copy.isEmpty();
    }

    @Test
    public void getGoogleS2CellIDsPoint() {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2));
        Long[] cid = Functions.s2CellIDs(point, 30);
        Polygon reversedPolygon = S2Utils.toJTSPolygon(new S2CellId(cid[0]));
        // cast the cell to a rectangle, it must be able to cover the points
        assert(reversedPolygon.contains(point));
    }

    @Test
    public void getGoogleS2CellIDsPolygon() {
        // polygon with holes
        Polygon target = GEOMETRY_FACTORY.createPolygon(
                GEOMETRY_FACTORY.createLinearRing(coordArray(0.1, 0.1, 0.5, 0.1, 1.0, 0.3, 1.0, 1.0, 0.1, 1.0, 0.1, 0.1)),
                new LinearRing[] {
                        GEOMETRY_FACTORY.createLinearRing(coordArray(0.2, 0.2, 0.5, 0.2, 0.6, 0.7, 0.2, 0.6, 0.2, 0.2))
                }
                );
        // polygon inside the hole, shouldn't intersect with the polygon
        Polygon polygonInHole = GEOMETRY_FACTORY.createPolygon(coordArray(0.3, 0.3, 0.4, 0.3, 0.3, 0.4, 0.3, 0.3));
        // mbr of the polygon that cover all
        Geometry mbr = target.getEnvelope();
        HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
        HashSet<Long> inHoleCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(polygonInHole, 10)));
        HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
        assert mbrCells.containsAll(targetCells);
        assert !intersects(targetCells, inHoleCells);
        assert mbrCells.containsAll(targetCells);
    }

    @Test
    public void getGoogleS2CellIDsLineString() {
        // polygon with holes
        LineString target = GEOMETRY_FACTORY.createLineString(coordArray(0.2, 0.2, 0.3, 0.4, 0.4, 0.6));
        LineString crossLine = GEOMETRY_FACTORY.createLineString(coordArray(0.4, 0.1, 0.1, 0.4));
        // mbr of the polygon that cover all
        Geometry mbr = target.getEnvelope();
        // cover the target polygon, and convert cells back to polygons
        HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 15)));
        HashSet<Long> crossCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(crossLine, 15)));
        HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 15)));
        assert intersects(targetCells, crossCells);
        assert mbrCells.containsAll(targetCells);
    }

    @Test
    public void getGoogleS2CellIDsMultiPolygon() {
        // polygon with holes
        Polygon[] geoms = new Polygon[] {
                GEOMETRY_FACTORY.createPolygon(coordArray(0.1, 0.1, 0.5, 0.1, 0.1, 0.6, 0.1, 0.1)),
                GEOMETRY_FACTORY.createPolygon(coordArray(0.2, 0.1, 0.6, 0.3, 0.7, 0.6, 0.2, 0.5, 0.2, 0.1))
        };
        MultiPolygon target = GEOMETRY_FACTORY.createMultiPolygon(geoms);
        Geometry mbr = target.getEnvelope();
        HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
        HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
        HashSet<Long> separateCoverCells = new HashSet<>();
        for(Geometry geom: geoms) {
            separateCoverCells.addAll(Arrays.asList(Functions.s2CellIDs(geom, 10)));
        }
        assert mbrCells.containsAll(targetCells);
        assert targetCells.equals(separateCoverCells);
    }

    @Test
    public void getGoogleS2CellIDsMultiLineString() {
        // polygon with holes
        MultiLineString target = GEOMETRY_FACTORY.createMultiLineString(
                new LineString[] {
                        GEOMETRY_FACTORY.createLineString(coordArray(0.1, 0.1, 0.2, 0.1, 0.3, 0.4, 0.5, 0.9)),
                        GEOMETRY_FACTORY.createLineString(coordArray(0.5, 0.1, 0.1, 0.5, 0.3, 0.1))
                }
        );
        Geometry mbr = target.getEnvelope();
        Point outsidePoint = GEOMETRY_FACTORY.createPoint(new Coordinate(0.3, 0.7));
        HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
        HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
        Long outsideCell = Functions.s2CellIDs(outsidePoint, 10)[0];
        // the cells should all be within mbr
        assert mbrCells.containsAll(targetCells);
        // verify point within mbr but shouldn't intersect with linestring
        assert mbrCells.contains(outsideCell);
        assert !targetCells.contains(outsideCell);
    }

    @Test
    public void getGoogleS2CellIDsMultiPoint() {
        // polygon with holes
        MultiPoint target = GEOMETRY_FACTORY.createMultiPoint(new Point[] {
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.1, 0.1)),
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.2, 0.1)),
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.3, 0.2)),
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.5, 0.4))
        });
        Geometry mbr = target.getEnvelope();
        Point outsidePoint = GEOMETRY_FACTORY.createPoint(new Coordinate(0.3, 0.7));
        HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
        HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
        // the cells should all be within mbr
        assert mbrCells.containsAll(targetCells);
        assert targetCells.size() == 4;
    }

    @Test
    public void getGoogleS2CellIDsGeometryCollection() {
        // polygon with holes
        Geometry[] geoms = new Geometry[] {
                GEOMETRY_FACTORY.createLineString(coordArray(0.1, 0.1, 0.2, 0.1, 0.3, 0.4, 0.5, 0.9)),
                GEOMETRY_FACTORY.createPolygon(coordArray(0.1, 0.1, 0.5, 0.1, 0.1, 0.6, 0.1, 0.1)),
                GEOMETRY_FACTORY.createMultiPoint(new Point[] {
                        GEOMETRY_FACTORY.createPoint(new Coordinate(0.1, 0.1)),
                        GEOMETRY_FACTORY.createPoint(new Coordinate(0.2, 0.1)),
                        GEOMETRY_FACTORY.createPoint(new Coordinate(0.3, 0.2)),
                        GEOMETRY_FACTORY.createPoint(new Coordinate(0.5, 0.4))
                })
        };
        GeometryCollection target = GEOMETRY_FACTORY.createGeometryCollection(geoms);
        Geometry mbr = target.getEnvelope();
        HashSet<Long> targetCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(target, 10)));
        HashSet<Long> mbrCells = new HashSet<>(Arrays.asList(Functions.s2CellIDs(mbr, 10)));
        HashSet<Long> separateCoverCells = new HashSet<>();
        for(Geometry geom: geoms) {
            separateCoverCells.addAll(Arrays.asList(Functions.s2CellIDs(geom, 10)));
        }
        // the cells should all be within mbr
        assert mbrCells.containsAll(targetCells);
        // separately cover should return same result as covered together
        assert separateCoverCells.equals(targetCells);
    }

    @Test
    public void getGoogleS2CellIDsAllSameLevel() {
        // polygon with holes
        GeometryCollection target = GEOMETRY_FACTORY.createGeometryCollection(
                new Geometry[]{
                        GEOMETRY_FACTORY.createPolygon(coordArray(0.3, 0.3, 0.4, 0.3, 0.3, 0.4, 0.3, 0.3)),
                        GEOMETRY_FACTORY.createPoint(new Coordinate(0.7, 1.2))
                }
        );
        Long[] cellIds = Functions.s2CellIDs(target, 10);
        HashSet<Integer> levels = Arrays.stream(cellIds).map(c -> new S2CellId(c).level()).collect(Collectors.toCollection(HashSet::new));
        HashSet<Integer> expects = new HashSet<>();
        expects.add(10);
        assertEquals(expects, levels);
    }

    @Test
    public void geometricMedian() throws Exception {
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(
                coordArray(1480,0, 620,0));
        Geometry actual = Functions.geometricMedian(multiPoint);
        Geometry expected = wktReader.read("POINT (1050 0)");
        assertEquals(0, expected.compareTo(actual, COORDINATE_SEQUENCE_COMPARATOR));
    }

    @Test
    public void geometricMedianTolerance() throws Exception {
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(
                coordArray(0,0, 10,1, 5,1, 20,20));
        Geometry actual = Functions.geometricMedian(multiPoint, 1e-15);
        Geometry expected = wktReader.read("POINT (5 1)");
        assertEquals(0, expected.compareTo(actual, COORDINATE_SEQUENCE_COMPARATOR));
    }

    @Test
    public void geometricMedianUnsupported() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(
                coordArray(1480,0, 620,0));
        Exception e = assertThrows(Exception.class, () -> Functions.geometricMedian(lineString));
        assertEquals("Unsupported geometry type: LineString", e.getMessage());
    }

    @Test
    public void geometricMedianFailConverge() {
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(
                coordArray(12,5, 62,7, 100,-1, 100,-5, 10,20, 105,-5));
        Exception e = assertThrows(Exception.class,
                () -> Functions.geometricMedian(multiPoint, 1e-6, 5, true));
        assertEquals("Median failed to converge within 1.0E-06 after 5 iterations.", e.getMessage());
    }

    @Test
    public void haversineDistance() {
        // Basic check
        Point p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 90));
        Point p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
        assertEquals(1.00075559643809E7, Haversine.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(51.3168, -0.56));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(55.9533, -3.1883));
        assertEquals(543796.9506134904, Haversine.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(48.353889, 11.786111));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(50.033333, 8.570556));
        assertEquals(299073.03416817175, Haversine.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(48.353889, 11.786111));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(52.559722, 13.287778));
        assertEquals(479569.4558072244, Haversine.distance(p1, p2), 0.1);

        LineString l1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 90));
        LineString l2 = GEOMETRY_FACTORY.createLineString(coordArray(0, 1, 0, 0));
        assertEquals(4948180.449055, Haversine.distance(l1, l2), 0.1);

        // HK to Sydney
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(22.308919, 113.914603));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-33.946111, 151.177222));
        assertEquals(7393893.072901942, Haversine.distance(p1, p2), 0.1);

        // HK to Toronto
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(22.308919, 113.914603));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(43.677223, -79.630556));
        assertEquals(1.2548548944238186E7, Haversine.distance(p1, p2), 0.1);
    }

    @Test
    public void spheroidDistance() {
        // Basic check
        Point p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 90));
        Point p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
        assertEquals(1.0018754171394622E7, Spheroid.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(51.3168, -0.56));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(55.9533, -3.1883));
        assertEquals(544430.9411996203, Spheroid.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(48.353889, 11.786111));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(50.033333, 8.570556));
        assertEquals(299648.07216251583, Spheroid.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(48.353889, 11.786111));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(52.559722, 13.287778));
        assertEquals(479817.9049528187, Spheroid.distance(p1, p2), 0.1);

        LineString l1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 90));
        LineString l2 = GEOMETRY_FACTORY.createLineString(coordArray(0, 1, 0, 0));
        assertEquals(4953717.340300673, Spheroid.distance(l1, l2), 0.1);

        // HK to Sydney
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(22.308919, 113.914603));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-33.946111, 151.177222));
        assertEquals(7371809.8295041, Spheroid.distance(p1, p2), 0.1);

        // HK to Toronto
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(22.308919, 113.914603));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(43.677223, -79.630556));
        assertEquals(1.2568775317073349E7, Spheroid.distance(p1, p2), 0.1);
    }

    @Test
    public void spheroidArea() {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 90));
        assertEquals(0, Spheroid.area(point), 0.1);

        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 90));
        assertEquals(0, Spheroid.area(line), 0.1);

        Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
        assertEquals(0, Spheroid.area(polygon1), 0.1);

        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(35, 34, 30, 28, 34, 25, 35, 34));
        assertEquals(2.0182485081176245E11, Spheroid.area(polygon2), 0.1);

        Polygon polygon3 = GEOMETRY_FACTORY.createPolygon(coordArray(35, 34, 34, 25, 30, 28, 35, 34));
        assertEquals(2.0182485081176245E11, Spheroid.area(polygon3), 0.1);

        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPoint(new Point[] { point, point });
        assertEquals(0, Spheroid.area(multiPoint), 0.1);

        MultiLineString multiLineString = GEOMETRY_FACTORY.createMultiLineString(new LineString[] { line, line });
        assertEquals(0, Spheroid.area(multiLineString), 0.1);

        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon2, polygon3});
        assertEquals(4.036497016235249E11, Spheroid.area(multiPolygon), 0.1);

        GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point, polygon2, polygon3});
        assertEquals(4.036497016235249E11, Spheroid.area(geometryCollection), 0.1);
    }

    @Test
    public void spheroidLength() {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 90));
        assertEquals(0, Spheroid.length(point), 0.1);

        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 90));
        assertEquals(1.0018754171394622E7, Spheroid.length(line), 0.1);

        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
        assertEquals(2.0037508342789244E7, Spheroid.length(polygon), 0.1);

        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPoint(new Point[] { point, point });
        assertEquals(0, Spheroid.length(multiPoint), 0.1);

        MultiLineString multiLineString = GEOMETRY_FACTORY.createMultiLineString(new LineString[] { line, line });
        assertEquals(2.0037508342789244E7, Spheroid.length(multiLineString), 0.1);

        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon, polygon});
        assertEquals(4.007501668557849E7, Spheroid.length(multiPolygon), 0.1);

        GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point, line, multiLineString});
        assertEquals(3.0056262514183864E7, Spheroid.length(geometryCollection), 0.1);
    }

    @Test
    public void numPoints() throws Exception{
        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 1, 1, 0, 2, 0));
        int expected = 3;
        int actual = Functions.numPoints(line);
        assertEquals(expected, actual);
    }

    @Test
    public void numPointsUnsupported() throws Exception {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
        String expected = "Unsupported geometry type: " + "Polygon" + ", only LineString geometry is supported.";
        Exception e = assertThrows(IllegalArgumentException.class, () -> Functions.numPoints(polygon));
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void force3DObject2D() {
        int expectedDims = 3;
        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 1, 1, 0, 2, 0));
        LineString expectedLine = GEOMETRY_FACTORY.createLineString(coordArray3d(0, 1, 1.1, 1, 0, 1.1, 2, 0, 1.1));
        Geometry forcedLine = Functions.force3D(line, 1.1);
        WKTWriter wktWriter = new WKTWriter(GeomUtils.getDimension(expectedLine));
        assertEquals(wktWriter.write(expectedLine), wktWriter.write(forcedLine));
        assertEquals(expectedDims, Functions.nDims(forcedLine));
    }

    @Test
    public void force3DObject2DDefaultValue() {
        int expectedDims = 3;
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
        Polygon expectedPolygon = GEOMETRY_FACTORY.createPolygon(coordArray3d(0, 0, 0, 0, 90, 0, 0, 0, 0));
        Geometry forcedPolygon = Functions.force3D(polygon);
        WKTWriter wktWriter = new WKTWriter(GeomUtils.getDimension(expectedPolygon));
        assertEquals(wktWriter.write(expectedPolygon), wktWriter.write(forcedPolygon));
        assertEquals(expectedDims, Functions.nDims(forcedPolygon));
    }

    @Test
    public void force3DObject3D() {
        int expectedDims = 3;
        LineString line3D = GEOMETRY_FACTORY.createLineString(coordArray3d(0, 1, 1, 1, 2, 1, 1, 2, 2));
        Geometry forcedLine3D = Functions.force3D(line3D, 2.0);
        WKTWriter wktWriter = new WKTWriter(GeomUtils.getDimension(line3D));
        assertEquals(wktWriter.write(line3D), wktWriter.write(forcedLine3D));
        assertEquals(expectedDims, Functions.nDims(forcedLine3D));
    }

    @Test
    public void force3DObject3DDefaultValue() {
        int expectedDims = 3;
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray3d(0, 0, 0, 90, 0, 0, 0, 0, 0));
        Geometry forcedPolygon = Functions.force3D(polygon);
        WKTWriter wktWriter = new WKTWriter(GeomUtils.getDimension(polygon));
        assertEquals(wktWriter.write(polygon), wktWriter.write(forcedPolygon));
        assertEquals(expectedDims, Functions.nDims(forcedPolygon));
    }

    @Test
    public void force3DEmptyObject() {
        LineString emptyLine = GEOMETRY_FACTORY.createLineString();
        Geometry forcedEmptyLine = Functions.force3D(emptyLine, 1.2);
        assertEquals(emptyLine.isEmpty(), forcedEmptyLine.isEmpty());
    }

    @Test
    public void force3DEmptyObjectDefaultValue() {
        LineString emptyLine = GEOMETRY_FACTORY.createLineString();
        Geometry forcedEmptyLine = Functions.force3D(emptyLine);
        assertEquals(emptyLine.isEmpty(), forcedEmptyLine.isEmpty());
    }

    @Test
    public void force3DHybridGeomCollection() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        Polygon polygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon3D, polygon});
        Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 1, 2));
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        Geometry geomCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {multiPolygon, point3D, emptyLineString, lineString})});
        Polygon expectedPolygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 2, 1, 1, 2, 2, 1, 2, 2, 0, 2, 1, 0, 2));
        LineString expectedLineString3D = GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 2, 1, 1, 2, 1, 2, 2));
        Geometry actualGeometryCollection = Functions.force3D(geomCollection, 2);
        WKTWriter wktWriter3D = new WKTWriter(3);
        assertEquals(wktWriter3D.write(polygon3D), wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(0).getGeometryN(0)));
        assertEquals(wktWriter3D.write(expectedPolygon3D), wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(0).getGeometryN(1)));
        assertEquals(wktWriter3D.write(point3D), wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(1)));
        assertEquals(emptyLineString.toText(), actualGeometryCollection.getGeometryN(0).getGeometryN(2).toText());
        assertEquals(wktWriter3D.write(expectedLineString3D), wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(3)));
    }

    @Test
    public void force3DHybridGeomCollectionDefaultValue() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        Polygon polygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon3D, polygon});
        Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 1, 2));
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        Geometry geomCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {multiPolygon, point3D, emptyLineString, lineString})});
        Polygon expectedPolygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 0, 1, 1, 0, 2, 1, 0, 2, 0, 0, 1, 0, 0));
        LineString expectedLineString3D = GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 0, 1, 1, 0, 1, 2, 0));
        Geometry actualGeometryCollection = Functions.force3D(geomCollection);
        WKTWriter wktWriter3D = new WKTWriter(3);
        assertEquals(wktWriter3D.write(polygon3D), wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(0).getGeometryN(0)));
        assertEquals(wktWriter3D.write(expectedPolygon3D), wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(0).getGeometryN(1)));
        assertEquals(wktWriter3D.write(point3D), wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(1)));
        assertEquals(emptyLineString.toText(), actualGeometryCollection.getGeometryN(0).getGeometryN(2).toText());
        assertEquals(wktWriter3D.write(expectedLineString3D), wktWriter3D.write(actualGeometryCollection.getGeometryN(0).getGeometryN(3)));
    }

    @Test
    public void nRingsPolygonOnlyExternal() throws Exception {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        Integer expected = 1;
        Integer actual = Functions.nRings(polygon);
        assertEquals(expected, actual);
    }

    @Test
    public void nRingsPolygonWithHoles() throws Exception {
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coordArray(1, 0, 1, 6, 6, 6, 6, 0, 1, 0));
        LinearRing[] holes = new LinearRing[] {GEOMETRY_FACTORY.createLinearRing(coordArray(2, 1, 2, 2, 3, 2, 3, 1, 2, 1)),
                                                GEOMETRY_FACTORY.createLinearRing(coordArray(4, 1, 4, 2, 5, 2, 5, 1, 4, 1))};
        Polygon polygonWithHoles = GEOMETRY_FACTORY.createPolygon(shell, holes);
        Integer expected = 3;
        Integer actual = Functions.nRings(polygonWithHoles);
        assertEquals(expected, actual);
    }

    @Test public void nRingsPolygonEmpty() throws Exception {
        Polygon emptyPolygon = GEOMETRY_FACTORY.createPolygon();
        Integer expected = 0;
        Integer actual = Functions.nRings(emptyPolygon);
        assertEquals(expected, actual);
    }

    @Test
    public void nRingsMultiPolygonOnlyExternal() throws Exception {
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0)),
                                                                                        GEOMETRY_FACTORY.createPolygon(coordArray(5, 0, 5, 1, 7, 1, 7, 0, 5, 0))});
        Integer expected = 2;
        Integer actual = Functions.nRings(multiPolygon);
        assertEquals(expected, actual);
    }

    @Test
    public void nRingsMultiPolygonOnlyWithHoles() throws Exception {
        LinearRing shell1 = GEOMETRY_FACTORY.createLinearRing(coordArray(1, 0, 1, 6, 6, 6, 6, 0, 1, 0));
        LinearRing[] holes1 = new LinearRing[] {GEOMETRY_FACTORY.createLinearRing(coordArray(2, 1, 2, 2, 3, 2, 3, 1, 2, 1)),
                GEOMETRY_FACTORY.createLinearRing(coordArray(4, 1, 4, 2, 5, 2, 5, 1, 4, 1))};
        Polygon polygonWithHoles1 = GEOMETRY_FACTORY.createPolygon(shell1, holes1);
        LinearRing shell2 = GEOMETRY_FACTORY.createLinearRing(coordArray(10, 0, 10, 6, 16, 6, 16, 0, 10, 0));
        LinearRing[] holes2 = new LinearRing[] {GEOMETRY_FACTORY.createLinearRing(coordArray(12, 1, 12, 2, 13, 2, 13, 1, 12, 1)),
                GEOMETRY_FACTORY.createLinearRing(coordArray(14, 1, 14, 2, 15, 2, 15, 1, 14, 1))};
        Polygon polygonWithHoles2 = GEOMETRY_FACTORY.createPolygon(shell2, holes2);
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{polygonWithHoles1, polygonWithHoles2});
        Integer expected = 6;
        Integer actual = Functions.nRings(multiPolygon);
        assertEquals(expected, actual);
    }

    @Test
    public void nRingsMultiPolygonEmpty() throws Exception {
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {GEOMETRY_FACTORY.createPolygon(),
                GEOMETRY_FACTORY.createPolygon()});
        Integer expected = 0;
        Integer actual = Functions.nRings(multiPolygon);
        assertEquals(expected, actual);
    }

    @Test
    public void nRingsMultiPolygonMixed() throws Exception {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coordArray(1, 0, 1, 6, 6, 6, 6, 0, 1, 0));
        LinearRing[] holes = new LinearRing[] {GEOMETRY_FACTORY.createLinearRing(coordArray(2, 1, 2, 2, 3, 2, 3, 1, 2, 1)),
                GEOMETRY_FACTORY.createLinearRing(coordArray(4, 1, 4, 2, 5, 2, 5, 1, 4, 1))};
        Polygon polygonWithHoles = GEOMETRY_FACTORY.createPolygon(shell, holes);
        Polygon emptyPolygon = GEOMETRY_FACTORY.createPolygon();
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon, polygonWithHoles, emptyPolygon});
        Integer expected = 4;
        Integer actual = Functions.nRings(multiPolygon);
        assertEquals(expected, actual);
    }

    @Test
    public void nRingsUnsupported() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray3d(0, 1, 1, 1, 2, 1, 1, 2, 2));
        String expected = "Unsupported geometry type: " + "LineString" + ", only Polygon or MultiPolygon geometries are supported.";
        Exception e = assertThrows(IllegalArgumentException.class, () -> Functions.nRings(lineString));
        assertEquals(expected, e.getMessage());
    }

    @Test
    public void translateEmptyObjectNoDeltaZ() {
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        String expected = emptyLineString.toText();
        String actual = Functions.translate(emptyLineString, 1, 1).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void translateEmptyObjectDeltaZ() {
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        String expected = emptyLineString.toText();
        String actual = Functions.translate(emptyLineString, 1, 3, 2).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void translate2DGeomNoDeltaZ() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        String expected = GEOMETRY_FACTORY.createPolygon(coordArray(2, 4, 2, 5, 3, 5, 3, 4, 2, 4)).toText();
        String actual = Functions.translate(polygon, 1, 4).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void translate2DGeomDeltaZ() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        String expected = GEOMETRY_FACTORY.createPolygon(coordArray(2, 3, 2, 4, 3, 4, 3, 3, 2, 3)).toText();
        String actual = Functions.translate(polygon, 1, 3, 2).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void translate3DGeomNoDeltaZ() {
        WKTWriter wktWriter = new WKTWriter(3);
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 1, 1, 1, 2, 1, 1, 2, 0, 1, 1, 0, 1));
        Polygon expectedPolygon = GEOMETRY_FACTORY.createPolygon(coordArray3d(2, 5, 1, 2, 6, 1, 3, 6, 1, 3, 5, 1, 2, 5, 1));
        assertEquals(wktWriter.write(expectedPolygon), wktWriter.write(Functions.translate(polygon, 1, 5)));
    }

    @Test
    public void translate3DGeomDeltaZ() {
        WKTWriter wktWriter = new WKTWriter(3);
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 1, 1, 1, 2, 1, 1, 2, 0, 1, 1, 0, 1));
        Polygon expectedPolygon = GEOMETRY_FACTORY.createPolygon(coordArray3d(2, 2, 4, 2, 3, 4, 3, 3, 4, 3, 2, 4, 2, 2, 4));
        assertEquals(wktWriter.write(expectedPolygon), wktWriter.write(Functions.translate(polygon, 1, 2, 3)));
    }

    @Test
    public void translateHybridGeomCollectionNoDeltaZ() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        Polygon polygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 2, 0, 2, 2, 1, 2, 1, 0, 1));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon3D, polygon});
        Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        Geometry geomCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {multiPolygon, point3D, emptyLineString})});
        Polygon expectedPolygon = GEOMETRY_FACTORY.createPolygon(coordArray(2, 2, 2, 3, 3, 3, 3, 2, 2, 2));
        Polygon expectedPolygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(2, 2, 1, 3, 2, 2, 3, 3, 2, 2, 2, 1));
        Point expectedPoint3D = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 3, 1));
        WKTWriter wktWriter3D = new WKTWriter(3);
        GeometryCollection actualGeometry = (GeometryCollection) Functions.translate(geomCollection, 1, 2);
        assertEquals(wktWriter3D.write(expectedPolygon3D), wktWriter3D.write(actualGeometry.getGeometryN(0).getGeometryN(0).getGeometryN(0)));
        assertEquals(expectedPolygon.toText(), actualGeometry.getGeometryN(0).getGeometryN(0).getGeometryN(1).toText());
        assertEquals(wktWriter3D.write(expectedPoint3D), wktWriter3D.write(actualGeometry.getGeometryN(0).getGeometryN(1)));
        assertEquals(emptyLineString.toText(), actualGeometry.getGeometryN(0).getGeometryN(2).toText());
    }

    @Test
    public void translateHybridGeomCollectionDeltaZ() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        Polygon polygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 2, 0, 2, 2, 1, 2, 1, 0, 1));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon3D, polygon});
        Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        Geometry geomCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {multiPolygon, point3D, emptyLineString})});
        Polygon expectedPolygon = GEOMETRY_FACTORY.createPolygon(coordArray(2, 3, 2, 4, 3, 4, 3, 3, 2, 3));
        Polygon expectedPolygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(2, 3, 6, 3, 3, 7, 3, 4, 7, 2, 3, 6));
        Point expectedPoint3D = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 4, 6));
        WKTWriter wktWriter3D = new WKTWriter(3);
        GeometryCollection actualGeometry = (GeometryCollection) Functions.translate(geomCollection, 1, 3, 5);

        assertEquals(wktWriter3D.write(expectedPolygon3D), wktWriter3D.write(actualGeometry.getGeometryN(0).getGeometryN(0).getGeometryN(0)));
        assertEquals(expectedPolygon.toText(), actualGeometry.getGeometryN(0).getGeometryN(0).getGeometryN(1).toText());
        assertEquals(wktWriter3D.write(expectedPoint3D), wktWriter3D.write(actualGeometry.getGeometryN(0).getGeometryN(1)));
        assertEquals(emptyLineString.toText(), actualGeometry.getGeometryN(0).getGeometryN(2).toText());
    }
    @Test
    public void boundingDiagonalGeom2D() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 2, 2, 0, 1, 0));
        String expected = "LINESTRING (1 0, 2 2)";
        String actual = Functions.boundingDiagonal(polygon).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void boundingDiagonalGeom3D() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 3, 2, 2, 2, 4, 5, 1, 1, 1, 1, 0, 1));
        WKTWriter wktWriter = new WKTWriter(3);
        String expected = "LINESTRING Z(1 0 1, 3 4 5)";
        String actual = wktWriter.write(Functions.boundingDiagonal(polygon));
        assertEquals(expected, actual);
    }

    @Test
    public void boundingDiagonalGeomEmpty() {
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        String expected = "LINESTRING EMPTY";
        String actual = Functions.boundingDiagonal(emptyLineString).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void boundingDiagonalGeomCollection2D() {
        //      ("'GEOMETRYCOLLECTION (MULTIPOLYGON (((1 1, 1 -1, 2 2, 2 9, 9 1, 1 1)), ((5 5, 4 4, 2 2 , 5 5))), POINT (-1 0))'") -> "'LINESTRING (-1 -1, 9 9)'"
        Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(1, 1, 1, -1, 2, 2, 2, 9, 9, 1, 1, 1));
        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(5, 5, 4, 4, 2, 2, 5, 5));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon1, polygon2});
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(2, 2, 3, 3));
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-1, 0));
        Geometry geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {multiPolygon, lineString, point});
        String expected = "LINESTRING (-1 -1, 9 9)";
        String actual = Functions.boundingDiagonal(geometryCollection).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void boundingDiagonalGeomCollection3D() {
        Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 1, 4, 1, -1, 6, 2, 2, 4, 2, 9, 4, 9, 1, 0, 1, 1, 4));
        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray3d(5, 5, 1, 4, 4, 1, 2, 2, 2, 5, 5, 1));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon1, polygon2});
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray3d(2, 2, 9, 3, 3, -5));
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(-1, 9, 1));
        Geometry geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {multiPolygon, lineString, point});
        String expected = "LINESTRING Z(-1 -1 -5, 9 9 9)";
        WKTWriter wktWriter = new WKTWriter(3);
        String actual = wktWriter.write(Functions.boundingDiagonal(geometryCollection));
        assertEquals(expected, actual);
    }

    @Test
    public void boundingDiagonalSingleVertex() {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(10, 5));
        String expected = "LINESTRING (10 5, 10 5)";
        String actual = Functions.boundingDiagonal(point).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void hausdorffDistanceDefaultGeom2D() throws Exception {
        Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 1, 1, 2, 2, 1, 5, 2, 0, 1, 1, 0, 1));
        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray3d(4, 0, 4, 6, 1, 4, 6, 4, 9, 6, 1, 3, 4, 0, 4));
        Double expected = 5.0;
        Double actual = Functions.hausdorffDistance(polygon1, polygon2);
        assertEquals(expected, actual);
    }

    @Test
    public void hausdorffDistanceGeom2D() throws Exception {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(10, 34));
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 2, 1, 5, 2, 6, 1, 2));
        Double expected = 33.24154027718932;
        Double actual = Functions.hausdorffDistance(point, lineString, 0.33);
        assertEquals(expected, actual);
    }

    @Test
    public void hausdorffDistanceInvalidDensityFrac() throws Exception {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(10, 34));
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 2, 1, 5, 2, 6, 1, 2));
        Exception e = assertThrows(IllegalArgumentException.class, () -> Functions.hausdorffDistance(point, lineString, 3));
        String expected = "Fraction is not in range (0.0 - 1.0]";
        String actual = e.getMessage();
        assertEquals(expected, actual);
    }

    @Test
    public void hausdorffDistanceDefaultGeomCollection() throws Exception {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 2, 2, 1, 2, 0, 4, 1, 1, 2));
        Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 0));
        Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(40, 10));
        Geometry point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(-10, -40));
        GeometryCollection multiPoint = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point1, point2, point3});
        Double actual = Functions.hausdorffDistance(polygon, multiPoint);
        Double expected = 41.7612260356422;
        assertEquals(expected, actual);
    }

    @Test
    public void hausdorffDistanceGeomCollection() throws Exception {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 2, 2, 1, 2, 0, 4, 1, 1, 2));
        LineString lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(1, 1, 2, 1, 4, 4, 5, 5));
        LineString lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(10, 10, 11, 11, 12, 12, 14, 14));
        LineString lineString3 = GEOMETRY_FACTORY.createLineString(coordArray(-11, -20, -11, -21, -15, -19));
        MultiLineString multiLineString = GEOMETRY_FACTORY.createMultiLineString(new LineString[] {lineString1, lineString2, lineString3});
        Double actual = Functions.hausdorffDistance(polygon, multiLineString, 0.0000001);
        Double expected = 25.495097567963924;
        assertEquals(expected, actual);
    }

    @Test
    public void hausdorffDistanceEmptyGeom() throws Exception {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 2, 2, 1, 2, 0, 4, 1, 1, 2));
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        Double expected = 0.0;
        Double actual = Functions.hausdorffDistance(polygon, emptyLineString, 0.00001);
        assertEquals(expected, actual);
    }

    @Test
    public void hausdorffDistanceDefaultEmptyGeom() throws Exception {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 2, 2, 1, 2, 0, 4, 1, 1, 2));
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        Double expected = 0.0;
        Double actual = Functions.hausdorffDistance(polygon, emptyLineString);
        assertEquals(expected, actual);
    }

}
