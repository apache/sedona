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
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.S2Utils;
import org.junit.Test;
import org.locationtech.jts.geom.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FunctionsTest {
    public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private Coordinate[] coordArray(double... coordValues) {
        Coordinate[] coords = new Coordinate[(int)(coordValues.length / 2)];
        for (int i = 0; i < coordValues.length; i += 2) {
            coords[(int)(i / 2)] = new Coordinate(coordValues[i], coordValues[i+1]);
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
}
