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
import org.apache.sedona.common.utils.H3Utils;
import com.google.common.math.DoubleMath;
import org.apache.sedona.common.sphere.Haversine;
import org.apache.sedona.common.sphere.Spheroid;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.S2Utils;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.projection.ProjectionException;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

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
    public void asEWKT() throws Exception{
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4236);
        Geometry geometry = geometryFactory.createPoint(new Coordinate(1.0, 2.0));
        String actualResult = Functions.asEWKT(geometry);
        String expectedResult = "SRID=4236;POINT (1 2)";
        Geometry actual = Constructors.geomFromEWKT(expectedResult);
        assertEquals(geometry, actual);
        assertEquals(expectedResult, actualResult);

        geometry = geometryFactory.createPoint(new Coordinate(1.0, 2.0, 3.0));
        actualResult = Functions.asEWKT(geometry);
        expectedResult = "SRID=4236;POINT Z(1 2 3)";
        actual = Constructors.geomFromEWKT(expectedResult);
        assertEquals(geometry, actual);
        assertEquals(expectedResult, actualResult);

        geometry = geometryFactory.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
        actualResult = Functions.asEWKT(geometry);
        expectedResult = "SRID=4236;POINT M(1 2 3)";
        actual = Constructors.geomFromEWKT(expectedResult);
        assertEquals(geometry, actual);
        assertEquals(expectedResult, actualResult);

        geometry = geometryFactory.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
        actualResult = Functions.asEWKT(geometry);
        expectedResult = "SRID=4236;POINT ZM(1 2 3 4)";
        actual = Constructors.geomFromEWKT(expectedResult);
        assertEquals(geometry, actual);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void asWKT() throws Exception {
        Geometry geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0));
        String actualResult = Functions.asWKT(geometry);
        String expectedResult = "POINT (1 2)";
        Geometry actual = Constructors.geomFromEWKT(expectedResult);
        assertEquals(geometry, actual);
        assertEquals(expectedResult, actualResult);

        geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
        actualResult = Functions.asWKT(geometry);
        expectedResult = "POINT Z(1 2 3)";
        actual = Constructors.geomFromEWKT(expectedResult);
        assertEquals(geometry, actual);
        assertEquals(expectedResult, actualResult);

        geometry = GEOMETRY_FACTORY.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
        actualResult = Functions.asWKT(geometry);
        expectedResult = "POINT M(1 2 3)";
        actual = Constructors.geomFromEWKT(expectedResult);
        assertEquals(geometry, actual);
        assertEquals(expectedResult, actualResult);

        geometry = GEOMETRY_FACTORY.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
        actualResult = Functions.asWKT(geometry);
        expectedResult = "POINT ZM(1 2 3 4)";
        actual = Constructors.geomFromEWKT(expectedResult);
        assertEquals(geometry, actual);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void asWKB() throws Exception{
        Geometry geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0));
        byte[] actualResult = Functions.asWKB(geometry);
        Geometry expected = Constructors.geomFromWKB(actualResult);
        assertEquals(expected, geometry);

        geometry = GEOMETRY_FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
        actualResult = Functions.asWKB(geometry);
        expected = Constructors.geomFromWKB(actualResult);
        assertEquals(expected, geometry);
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

    @Test
    public void dimensionGeometry2D() {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2));
        Integer actualResult = Functions.dimension(point);
        Integer expectedResult = 0;
        assertEquals(actualResult, expectedResult);

        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5));
        actualResult = Functions.dimension(lineString);
        expectedResult = 1;
        assertEquals(actualResult, expectedResult);

        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0));
        actualResult = Functions.dimension(polygon);
        expectedResult = 2;
        assertEquals(actualResult, expectedResult);

        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.0, 0.0, 1.0, 1.0));
        actualResult = Functions.dimension(multiPoint);
        expectedResult = 0;
        assertEquals(actualResult, expectedResult);

        LineString lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(1.0, 1.0, 2.0, 2.0));
        MultiLineString multiLineString = GEOMETRY_FACTORY
                .createMultiLineString(new LineString[] { lineString, lineString2 });
        actualResult = Functions.dimension(multiLineString);
        expectedResult = 1;
        assertEquals(actualResult, expectedResult);

        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] { polygon, polygon2 });
        actualResult = Functions.dimension(multiPolygon);
        expectedResult = 2;
        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void dimensionGeometry3D() {
        Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
        Integer actualResult = Functions.dimension(point3D);
        Integer expectedResult = 0;
        assertEquals(actualResult, expectedResult);

        LineString lineString3D = GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 1, 1, 1, 2));
        actualResult = Functions.dimension(lineString3D);
        expectedResult = 1;
        assertEquals(actualResult, expectedResult);

        Polygon polygon3D = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1));
        actualResult = Functions.dimension(polygon3D);
        expectedResult = 2;
        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void dimensionGeometryCollection() {
        Geometry[] geometry = new Geometry[] {
                GEOMETRY_FACTORY.createLineString(coordArray(0.0, 0.0, 1.5, 1.5)),
                GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0))
        };
        GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(geometry);

        Integer actualResult = Functions.dimension(geometryCollection);
        Integer expectedResult = 2;
        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void dimensionGeometryEmpty() {
        GeometryCollection emptyGeometryCollection = GEOMETRY_FACTORY.createGeometryCollection();

        Integer actualResult = Functions.dimension(emptyGeometryCollection);
        Integer expectedResult = 0;
        assertEquals(actualResult, expectedResult);
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

    /**
     * Test H3CellIds: pass in all the types of geometry, test if the function cover
     */
    @Test
    public void h3CellIDs() {
        Geometry[] combinedGeoms = new Geometry[] {
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.1, 0.1)),
                GEOMETRY_FACTORY.createPoint(new Coordinate(0.2, 0.1)),
                GEOMETRY_FACTORY.createLineString(coordArray(0.1, 0.1, 0.2, 0.1, 0.3, 0.4, 0.5, 0.9)),
                GEOMETRY_FACTORY.createLineString(coordArray(0.5, 0.1, 0.1, 0.5, 0.3, 0.1)),
                GEOMETRY_FACTORY.createPolygon(coordArray(0.1, 0.1, 0.5, 0.1, 0.1, 0.6, 0.1, 0.1)),
                GEOMETRY_FACTORY.createPolygon(coordArray(0.2, 0.1, 0.6, 0.3, 0.7, 0.6, 0.2, 0.5, 0.2, 0.1))
        };
        // The test geometries, cover all 7 geometry types targeted
        Geometry[] targets = new Geometry[] {
                GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2)),
                GEOMETRY_FACTORY.createPolygon(
                        GEOMETRY_FACTORY.createLinearRing(coordArray(0.1, 0.1, 0.5, 0.1, 1.0, 0.3, 1.0, 1.0, 0.1, 1.0, 0.1, 0.1)),
                        new LinearRing[] {
                                GEOMETRY_FACTORY.createLinearRing(coordArray(0.2, 0.2, 0.5, 0.2, 0.6, 0.7, 0.2, 0.6, 0.2, 0.2))
                        }
                ),
                GEOMETRY_FACTORY.createLineString(coordArray(0.2, 0.2, 0.3, 0.4, 0.4, 0.6)),
                GEOMETRY_FACTORY.createGeometryCollection(
                        new Geometry[] {
                                GEOMETRY_FACTORY.createMultiPoint(new Point[] {(Point) combinedGeoms[0], (Point) combinedGeoms[1]}),
                                GEOMETRY_FACTORY.createMultiLineString(new LineString[] {(LineString) combinedGeoms[2], (LineString) combinedGeoms[3]}),
                                GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {(Polygon) combinedGeoms[4], (Polygon) combinedGeoms[5]})
                        }
                )
        };
        int resolution = 7;
        // the expected results
        Set<Long> expects = new HashSet<>();
        expects.addAll(new HashSet<>(Collections.singletonList(H3Utils.coordinateToCell(targets[0].getCoordinate(), resolution))));
        expects.addAll(new HashSet<>(H3Utils.polygonToCells((Polygon) targets[1], resolution, true)));
        expects.addAll(new HashSet<>(H3Utils.lineStringToCells((LineString) targets[2], resolution, true)));
        // for GeometryCollection, generate separately for the underlying geoms
        expects.add(H3Utils.coordinateToCell(combinedGeoms[0].getCoordinate(), resolution));
        expects.add(H3Utils.coordinateToCell(combinedGeoms[1].getCoordinate(), resolution));
        expects.addAll(H3Utils.lineStringToCells((LineString) combinedGeoms[2], resolution, true));
        expects.addAll(H3Utils.lineStringToCells((LineString) combinedGeoms[3], resolution, true));
        expects.addAll(H3Utils.polygonToCells((Polygon) combinedGeoms[4], resolution, true));
        expects.addAll(H3Utils.polygonToCells((Polygon) combinedGeoms[5], resolution, true));
        // generate exact
        Set<Long> exacts = new HashSet<>(Arrays.asList(Functions.h3CellIDs(GEOMETRY_FACTORY.createGeometryCollection(targets), resolution, true)));
        assert exacts.equals(expects);
    }

    /**
     * Test H3CellDistance
     */
    @Test
    public void h3CellDistance() {
        LineString pentagonLine = GEOMETRY_FACTORY.createLineString(coordArray(58.174758948493505, 10.427371502467615, 58.1388817207103, 10.469490838693966));
        // normal line
        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(-4.414062499999996, 19.790494005157534,5.781250000000004, 13.734595619093557));
        long pentagonDist = Functions.h3CellDistance(
                H3Utils.coordinateToCell(pentagonLine.getCoordinateN(0), 11),
                H3Utils.coordinateToCell(pentagonLine.getCoordinateN(1), 11)
        );
        long lineDist = Functions.h3CellDistance(
                H3Utils.coordinateToCell(line.getCoordinateN(0), 10),
                H3Utils.coordinateToCell(line.getCoordinateN(1), 10)
        );
        assertEquals(
                H3Utils.approxPathCells(
                        pentagonLine.getCoordinateN(0),
                        pentagonLine.getCoordinateN(1),
                        11,
                        true
                ).size() - 1,
                pentagonDist
        );

        assertEquals(
                H3Utils.h3.gridDistance(
                        H3Utils.coordinateToCell(line.getCoordinateN(0), 10),
                        H3Utils.coordinateToCell(line.getCoordinateN(1), 10)
                ),
                lineDist
        );
    }

    /**
     * Test h3kRing
     */
    @Test
    public void h3KRing() {
        Point[] points = new Point[] {
                // pentagon
                GEOMETRY_FACTORY.createPoint(new Coordinate(10.53619907546767, 64.70000012793487)),
                // 7th neighbor of pentagon
                GEOMETRY_FACTORY.createPoint(new Coordinate(10.536630883471666, 64.69944253201858)),
                // normal point
                GEOMETRY_FACTORY.createPoint(new Coordinate(-166.093005914,61.61964122848931)),
        };
        for (Point point : points) {
            long cell = H3Utils.coordinateToCell(point.getCoordinate(), 12);
            Set<Long> allNeighbors = new HashSet<>(Arrays.asList(Functions.h3KRing(cell, 10, false)));
            Set<Long> kthNeighbors = new HashSet<>(Arrays.asList(Functions.h3KRing(cell, 10, true)));
            assert allNeighbors.containsAll(kthNeighbors);
            kthNeighbors.addAll(Arrays.asList(Functions.h3KRing(cell, 9, false)));
            assert allNeighbors.equals(kthNeighbors);
        }
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
    public void makepolygonWithSRID() {
        Geometry lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 1, 1, 1, 0, 0, 0));
        Geometry actual1 = Functions.makepolygonWithSRID(lineString1, 4326);
        Geometry expected1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 1, 1, 1, 0, 0, 0));
        assertEquals(expected1.toText(), actual1.toText());
        assertEquals(4326, actual1.getSRID());

        Geometry lineString2 = GEOMETRY_FACTORY.createLineString(coordArray3d(75, 29, 1, 77, 29, 2, 77, 29, 3, 75, 29, 1));
        Geometry actual2 = Functions.makepolygonWithSRID(lineString2, 123);
        Geometry expected2 = GEOMETRY_FACTORY.createPolygon(coordArray3d(75, 29, 1, 77, 29, 2, 77, 29, 3, 75, 29, 1));
        assertEquals(expected2.toText(), actual2.toText());
        assertEquals(123, actual2.getSRID());
    }

    @Test
    public void haversineDistance() {
        // Basic check
        Point p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(90, 0));
        Point p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
        assertEquals(1.00075559643809E7, Haversine.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-0.56, 51.3168));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-3.1883, 55.9533));
        assertEquals(543796.9506134904, Haversine.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(11.786111, 48.353889));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(8.570556, 50.033333));
        assertEquals(299073.03416817175, Haversine.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(11.786111, 48.353889));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(13.287778, 52.559722));
        assertEquals(479569.4558072244, Haversine.distance(p1, p2), 0.1);

        LineString l1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 90));
        LineString l2 = GEOMETRY_FACTORY.createLineString(coordArray(0, 1, 0, 0));
        assertEquals(4948180.449055, Haversine.distance(l1, l2), 0.1);

        l1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 90, 0));
        l2 = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 0, 0));
        assertEquals(4948180.449055, Haversine.distance(l1, l2), 0.1);

        // HK to Sydney
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(113.914603, 22.308919));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(151.177222, -33.946111));
        assertEquals(7393893.072901942, Haversine.distance(p1, p2), 0.1);

        // HK to Toronto
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(113.914603, 22.308919));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-79.630556, 43.677223));
        assertEquals(1.2548548944238186E7, Haversine.distance(p1, p2), 0.1);

        // Crossing the anti-meridian
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, 0));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, 0));
        assertTrue(Haversine.distance(p1, p2) < 300);
        assertTrue(Haversine.distance(p2, p1) < 300);
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, 60));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, 60));
        assertTrue(Haversine.distance(p1, p2) < 300);
        assertTrue(Haversine.distance(p2, p1) < 300);
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, -60));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, -60));
        assertTrue(Haversine.distance(p1, p2) < 300);
        assertTrue(Haversine.distance(p2, p1) < 300);

        // Crossing the North Pole
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-60, 89.999));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(120, 89.999));
        assertTrue(Haversine.distance(p1, p2) < 300);
        assertTrue(Haversine.distance(p2, p1) < 300);

        // Crossing the South Pole
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-60, -89.999));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(120, -89.999));
        assertTrue(Haversine.distance(p1, p2) < 300);
        assertTrue(Haversine.distance(p2, p1) < 300);
    }

    @Test
    public void spheroidDistance() {
        // Basic check
        Point p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(90, 0));
        Point p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
        assertEquals(1.0018754171394622E7, Spheroid.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-0.56, 51.3168));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-3.1883, 55.9533));
        assertEquals(544430.9411996203, Spheroid.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(11.786111, 48.353889));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(8.570556, 50.033333));
        assertEquals(299648.07216251583, Spheroid.distance(p1, p2), 0.1);

        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(11.786111, 48.353889));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(13.287778, 52.559722));
        assertEquals(479817.9049528187, Spheroid.distance(p1, p2), 0.1);

        LineString l1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 90, 0));
        LineString l2 = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 0, 0));
        assertEquals(4953717.340300673, Spheroid.distance(l1, l2), 0.1);

        // HK to Sydney
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(113.914603, 22.308919));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(151.177222, -33.946111));
        assertEquals(7371809.8295041, Spheroid.distance(p1, p2), 0.1);

        // HK to Toronto
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(113.914603, 22.308919));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-79.630556, 43.677223));
        assertEquals(1.2568775317073349E7, Spheroid.distance(p1, p2), 0.1);

        // Crossing the anti-meridian
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, 0));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, 0));
        assertTrue(Spheroid.distance(p1, p2) < 300);
        assertTrue(Spheroid.distance(p2, p1) < 300);
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, 60));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, 60));
        assertTrue(Spheroid.distance(p1, p2) < 300);
        assertTrue(Spheroid.distance(p2, p1) < 300);
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(179.999, -60));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(-179.999, -60));
        assertTrue(Spheroid.distance(p1, p2) < 300);
        assertTrue(Spheroid.distance(p2, p1) < 300);

        // Crossing the North Pole
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-60, 89.999));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(120, 89.999));
        assertTrue(Spheroid.distance(p1, p2) < 300);
        assertTrue(Spheroid.distance(p2, p1) < 300);

        // Crossing the South Pole
        p1 = GEOMETRY_FACTORY.createPoint(new Coordinate(-60, -89.999));
        p2 = GEOMETRY_FACTORY.createPoint(new Coordinate(120, -89.999));
        assertTrue(Spheroid.distance(p1, p2) < 300);
        assertTrue(Spheroid.distance(p2, p1) < 300);
    }

    @Test
    public void spheroidArea() {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(90, 0));
        assertEquals(0, Spheroid.area(point), 0.1);

        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 90));
        assertEquals(0, Spheroid.area(line), 0.1);
        line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 90, 0));
        assertEquals(0, Spheroid.area(line), 0.1);

        Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
        assertEquals(0, Spheroid.area(polygon1), 0.1);
        polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 90, 0, 0, 0));
        assertEquals(0, Spheroid.area(polygon1), 0.1);

        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(34, 35, 28, 30, 25, 34, 34, 35));
        assertEquals(2.0182485081176245E11, Spheroid.area(polygon2), 0.1);

        Polygon polygon3 = GEOMETRY_FACTORY.createPolygon(coordArray(34, 35, 25, 34, 28, 30, 34, 35));
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
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(90, 0));
        assertEquals(0, Spheroid.length(point), 0.1);

        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 90, 0));
        assertEquals(1.0018754171394622E7, Spheroid.length(line), 0.1);

        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 90, 0, 0, 0));
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
    public void makeLine() {
        Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
        Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        String actual = Functions.makeLine(point1, point2).toText();
        assertEquals("LINESTRING (0 0, 1 1)", actual);

        MultiPoint multiPoint1 = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 1.0, 1.0));
        MultiPoint multiPoint2 = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0.5, 0.5, 2, 2));
        String actual2 = Functions.makeLine(multiPoint1, multiPoint2).toText();
        assertEquals("LINESTRING (0.5 0.5, 1 1, 0.5 0.5, 2 2)", actual2);

        LineString line1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 1, 1));
        LineString line2 = GEOMETRY_FACTORY.createLineString(coordArray(2, 2, 3, 3));
        Geometry[] geoms = new Geometry[]{line1, line2};
        String actual3 = Functions.makeLine(geoms).toText();
        assertEquals("LINESTRING (0 0, 1 1, 2 2, 3 3)", actual3);
    }

    @Test
    public void makeLine3d() {
        WKTWriter wktWriter3D = new WKTWriter(3);
        Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
        Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 2, 2));
        LineString actualLinestring = (LineString) Functions.makeLine(point1, point2);
        LineString expectedLineString = GEOMETRY_FACTORY.createLineString(coordArray3d( 1, 1, 1, 2, 2, 2));
        assertEquals(wktWriter3D.write(actualLinestring), wktWriter3D.write(expectedLineString));

        MultiPoint multiPoint1 = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray3d(0.5, 0.5, 1, 1, 1, 1));
        MultiPoint multiPoint2 = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray3d(0.5, 0.5, 2, 2, 2, 2));
        actualLinestring = (LineString) Functions.makeLine(multiPoint1, multiPoint2);
        expectedLineString = GEOMETRY_FACTORY.createLineString(coordArray3d( 0.5, 0.5, 1, 1, 1, 1, 0.5, 0.5, 2, 2, 2, 2));
        assertEquals(wktWriter3D.write(actualLinestring), wktWriter3D.write(expectedLineString));

        LineString line1 = GEOMETRY_FACTORY.createLineString(coordArray3d(0, 0, 1, 1, 1, 1));
        LineString line2 = GEOMETRY_FACTORY.createLineString(coordArray3d(2, 2, 2, 2, 3, 3));
        Geometry[] geoms = new Geometry[]{line1, line2};
        actualLinestring = (LineString) Functions.makeLine(geoms);
        expectedLineString = GEOMETRY_FACTORY.createLineString(coordArray3d( 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3));
        assertEquals(wktWriter3D.write(actualLinestring), wktWriter3D.write(expectedLineString));
    }

    @Test
    public void makeLineWithWrongType() {
        Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 90, 0, 0));
        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 10, 10, 10, 10, 0, 0, 0));

        Exception e = assertThrows(IllegalArgumentException.class, () -> Functions.makeLine(polygon1, polygon2));
        assertEquals("ST_MakeLine only supports Point, MultiPoint and LineString geometries", e.getMessage());
    }

    @Test
    public void minimumBoundingRadius() {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
        assertEquals("POINT (0 0)", Functions.minimumBoundingRadius(point).getLeft().toString());
        assertEquals(0, Functions.minimumBoundingRadius(point).getRight(), 1e-6);

        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 0, 10));
        assertEquals("POINT (0 5)", Functions.minimumBoundingRadius(line).getLeft().toString());
        assertEquals(5, Functions.minimumBoundingRadius(line).getRight(), 1e-6);

        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 10, 10, 10, 10, 0, 0, 0));
        assertEquals("POINT (5 5)", Functions.minimumBoundingRadius(polygon).getLeft().toString());
        assertEquals(7.071067, Functions.minimumBoundingRadius(polygon).getRight(), 1e-6);
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
    public void testBuffer() {
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(50, 50, 50, 150, 150, 150, 150, 50, 50, 50));
        String actual = Functions.asWKT(Functions.buffer(polygon, 15));
        String expected = "POLYGON ((50 35, 47.07364516975807 35.288220793951545, 44.25974851452364 36.1418070123307, 41.666446504705966 37.52795581546182, 39.39339828220179 39.39339828220179, 37.527955815461816 41.66644650470597, 36.141807012330695 44.25974851452366, 35.288220793951545 47.07364516975807, 35 50, 35 150, 35.288220793951545 152.92635483024193, 36.1418070123307 155.74025148547634, 37.52795581546182 158.33355349529404, 39.39339828220179 160.6066017177982, 41.66644650470597 162.4720441845382, 44.25974851452365 163.8581929876693, 47.07364516975808 164.71177920604845, 50 165, 150 165, 152.92635483024193 164.71177920604845, 155.74025148547634 163.8581929876693, 158.33355349529404 162.4720441845382, 160.6066017177982 160.6066017177982, 162.4720441845382 158.33355349529404, 163.8581929876693 155.74025148547634, 164.71177920604845 152.92635483024193, 165 150, 165 50, 164.71177920604845 47.07364516975807, 163.8581929876693 44.25974851452365, 162.4720441845382 41.666446504705966, 160.6066017177982 39.39339828220179, 158.33355349529404 37.52795581546182, 155.74025148547634 36.1418070123307, 152.92635483024193 35.288220793951545, 150 35, 50 35))";
        assertEquals(expected, actual);

        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 50, 70, 100, 100));
        actual = Functions.asWKT(Functions.buffer(lineString, 10, "side=left"));
        expected = "POLYGON ((100 100, 50 70, 0 0, -8.137334712067348 5.812381937190963, 41.86266528793265 75.81238193719096, 43.21673095875923 77.34760240582902, 44.855042445724735 78.57492925712545, 94.85504244572473 108.57492925712545, 100 100))";
        assertEquals(expected, actual);

        lineString = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 50, 70, 70, -3));
        actual = Functions.asWKT(Functions.buffer(lineString, 10, "endcap=square"));
        expected = "POLYGON ((41.86266528793265 75.81238193719096, 43.21555008457904 77.3465120530184, 44.85228625762473 78.57327494173381, 46.70439518001618 79.44134465372912, 48.69438734657371 79.914402432785, 50.73900442057982 79.9726562392556, 52.75270263976913 79.6136688198111, 54.65123184115194 78.8524596785218, 56.355160363552315 77.72087668296376, 57.79319835113832 76.26626359641972, 58.90518041582699 74.54947928466231, 59.64458286836891 72.642351470786, 79.64458286836891 -0.3576485292139977, 82.28693433915491 -10.002231397582907, 62.997768602417096 -15.28693433915491, 45.912786454208465 47.07325050180659, 8.137334712067348 -5.812381937190963, 2.324952774876386 -13.949716649258315, -13.94971664925831 -2.3249527748763885, 41.86266528793265 75.81238193719096))";
        assertEquals(expected, actual);

        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(100, 90));
        actual = Functions.asWKT(Functions.buffer(point, 10, "quad_segs=2"));
        expected = "POLYGON ((110 90, 107.07106781186548 82.92893218813452, 100 80, 92.92893218813452 82.92893218813452, 90 90, 92.92893218813452 97.07106781186548, 100 100, 107.07106781186548 97.07106781186548, 110 90))";
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
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{polygon3D, polygon});
        Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        Geometry geomCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[]{GEOMETRY_FACTORY.createGeometryCollection(new Geometry[]{multiPolygon, point3D, emptyLineString})});
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
    public void testFrechetGeom2D() {
        LineString lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 100, 0));
        LineString lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 50, 50, 100, 0));
        double expected = 70.7106781186548;
        double actual = Functions.frechetDistance(lineString1, lineString2);
        assertEquals(expected, actual, 1e-9);
    }

    @Test
    public void testFrechetGeom3D() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 1, 2, 2, 2, 3, 3, 3));
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 0, 1, 1, 1, 2, 1, 1, 1, 0, 0));
        double expected = 3.605551275463989;
        double actual = Functions.frechetDistance(lineString, polygon);
        assertEquals(expected, actual, 1e-9);
    }

    @Test
    public void testFrechetGeomCollection() {
        Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2));
        Geometry lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(2, 2, 3, 3, 4, 4));
        Geometry lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(-1, -1, -4, -4, -10, -10));
        Geometry geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point, lineString1, lineString2});
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        double expected = 14.866068747318506;
        double actual = Functions.frechetDistance(polygon, geometryCollection);
        assertEquals(expected, actual, 1e-9);
    }

    @Test
    public void testFrechetGeomEmpty() {
        Polygon p1 = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        LineString emptyPoint = GEOMETRY_FACTORY.createLineString();
        double expected = 0.0;
        double actual = Functions.frechetDistance(p1, emptyPoint);
        assertEquals(expected, actual, 1e-9);
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
    public void angleFourPoints() {
        Point start1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
        Point end1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        Point start2 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 0));
        Point end2 = GEOMETRY_FACTORY.createPoint(new Coordinate(6, 2));

        double expected = 0.4048917862850834;
        double expectedDegrees = 23.198590513648185;
        double reverseExpectedDegrees = 336.8014094863518;
        double reverseExpected = 5.878293520894503;

        double actualPointsFour = Functions.angle(start1, end1, start2, end2);
        double actualPointsFourDegrees = Functions.degrees(actualPointsFour);
        double actualPointsFourReverse = Functions.angle(start2, end2, start1, end1);
        double actualPointsFourReverseDegrees = Functions.degrees(actualPointsFourReverse);

        assertEquals(expected, actualPointsFour, 1e-9);
        assertEquals(expectedDegrees, actualPointsFourDegrees, 1e-9);
        assertEquals(reverseExpected, actualPointsFourReverse, 1e-9);
        assertEquals(reverseExpectedDegrees, actualPointsFourReverseDegrees, 1e-9);
    }

    @Test
    public void angleFourPoints3D() {
        Point start1 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0, 4));
        Point end1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 5));
        Point start2 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 0, 9));
        Point end2 = GEOMETRY_FACTORY.createPoint(new Coordinate(6, 2, 2));

        double expected = 0.4048917862850834;
        double expectedDegrees = 23.198590513648185;
        double reverseExpectedDegrees = 336.8014094863518;
        double reverseExpected = 5.878293520894503;

        double actualPointsFour = Functions.angle(start1, end1, start2, end2);
        double actualPointsFourDegrees = Functions.degrees(actualPointsFour);
        double actualPointsFourReverse = Functions.angle(start2, end2, start1, end1);
        double actualPointsFourReverseDegrees = Functions.degrees(actualPointsFourReverse);

        assertEquals(expected, actualPointsFour, 1e-9);
        assertEquals(expectedDegrees, actualPointsFourDegrees, 1e-9);
        assertEquals(reverseExpected, actualPointsFourReverse, 1e-9);
        assertEquals(reverseExpectedDegrees, actualPointsFourReverseDegrees, 1e-9);
    }



    @Test
    public void angleThreePoints() {
        Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0));
        Point point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(3, 2));

        double expected = 0.19739555984988044;
        double expectedDegrees = 11.309932474020195;


        double actualPointsThree = Functions.angle(point1, point2, point3);
        double actualPointsThreeDegrees = Functions.degrees(actualPointsThree);

        assertEquals(expected, actualPointsThree, 1e-9);
        assertEquals(expectedDegrees, actualPointsThreeDegrees, 1e-9);

    }

    @Test
    public void angleTwoLineStrings() {
        LineString lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 1, 1));
        LineString lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(0, 0, 3, 2));

        double expected = 0.19739555984988044;
        double expectedDegrees = 11.309932474020195;
        double reverseExpected = 6.085789747329706;
        double reverseExpectedDegrees = 348.69006752597977;

        double actualLineString = Functions.angle(lineString1, lineString2);
        double actualLineStringReverse = Functions.angle(lineString2, lineString1);
        double actualLineStringDegrees = Functions.degrees(actualLineString);
        double actualLineStringReverseDegrees = Functions.degrees(actualLineStringReverse);

        assertEquals(expected, actualLineString, 1e-9);
        assertEquals(reverseExpected, actualLineStringReverse, 1e-9);
        assertEquals(expectedDegrees, actualLineStringDegrees, 1e-9);
        assertEquals(reverseExpectedDegrees, actualLineStringReverseDegrees, 1e-9);
    }

    @Test
    public void angleInvalidEmptyGeom() {
        Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(3, 5));
        Point point2 = GEOMETRY_FACTORY.createPoint();
        Point point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));

        Exception e = assertThrows(IllegalArgumentException.class, () -> Functions.angle(point1, point2, point3));
        assertEquals("ST_Angle cannot support empty geometries.", e.getMessage());
    }

    @Test
    public void angleInvalidUnsupportedGeom() {
        Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(3, 5));
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        Point point3 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));

        Exception e = assertThrows(IllegalArgumentException.class, () -> Functions.angle(point1, polygon, point3));
        assertEquals("ST_Angle supports either only POINT or only LINESTRING geometries.", e.getMessage());
    }

    @Test
    public void isCollectionWithCollection() {
        Point[] points = new Point[]{
                GEOMETRY_FACTORY.createPoint(new Coordinate(5.0, 6.0)),
                GEOMETRY_FACTORY.createPoint(new Coordinate(7.0, 8.0))
        };
        GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(points);

        boolean actualResult = Functions.isCollection(geometryCollection);
        boolean expectedResult = true;
        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void isCollectionWithOutCollection() {
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(6.0, 6.0));

        boolean actualResult = Functions.isCollection(point);
        boolean expectedResult = false;
        assertEquals(actualResult, expectedResult);
    }

    public void affineEmpty3D() {
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        String expected = emptyLineString.toText();
        String actual = Functions.affine(emptyLineString, 1.0, 1.0, 4.0, 2.0, 2.0, 4.0, 5.0, 5.0, 6.0, 3.0, 3.0, 6.0).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void affineEmpty2D() {
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        String expected = emptyLineString.toText();
        String actual = Functions.affine(emptyLineString, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void affine3DGeom2D() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 1, 2));
        String expected = GEOMETRY_FACTORY.createLineString(coordArray(4, 5, 5, 7, 6, 9)).toText();
        String actual = Functions.affine(lineString, 1.0, 1.0, 4.0, 2.0, 2.0, 4.0, 5.0, 5.0, 6.0, 3.0, 3.0, 6.0).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void affine3DGeom3D() {
        WKTWriter wktWriter = new WKTWriter(3);
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 1, 1, 1, 2, 1, 2, 2));
        String expected = wktWriter.write(GEOMETRY_FACTORY.createLineString(coordArray3d(8, 9, 17, 13, 15, 28, 14, 17, 33)));
        String actual = wktWriter.write(Functions.affine(lineString, 1.0, 1.0, 4.0, 2.0, 2.0, 4.0, 5.0, 5.0, 6.0, 3.0, 3.0, 6.0));
        assertEquals(expected, actual);
    }

    @Test
    public void affine3DHybridGeomCollection() {
        Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1, 1));
        Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 2, 1, 1, 2, 2, 1, 2, 2, 0, 2, 1, 0, 2));
        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray3d(1, 0, 1, 1, 1, 1, 2, 2, 2, 1, 0, 1));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon1, polygon2});
        Geometry geomCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point3D, multiPolygon})});
        Geometry actualGeomCollection = Functions.affine(geomCollection, 1.0, 2.0, 1.0, 3.0, 1.0, 2.0, 3.0, 1.0, 2.0, 2.0, 3.0, 3.0);
        WKTWriter wktWriter3D = new WKTWriter(3);
        Point expectedPoint3D = GEOMETRY_FACTORY.createPoint(new Coordinate(6, 9, 9));
        Polygon expectedPolygon1 = GEOMETRY_FACTORY.createPolygon(coordArray3d(5, 10, 10,7, 11, 11, 8, 14, 14, 6, 13, 13, 5, 10, 10));
        Polygon expectedPolygon2 = GEOMETRY_FACTORY.createPolygon(coordArray3d(4, 8, 8, 6, 9, 9, 10, 15, 15, 4, 8, 8));
        assertEquals(wktWriter3D.write(expectedPoint3D), wktWriter3D.write(actualGeomCollection.getGeometryN(0).getGeometryN(0)));
        assertEquals(wktWriter3D.write(expectedPolygon1), wktWriter3D.write(actualGeomCollection.getGeometryN(0).getGeometryN(1).getGeometryN(0)));
        assertEquals(wktWriter3D.write(expectedPolygon2), wktWriter3D.write(actualGeomCollection.getGeometryN(0).getGeometryN(1).getGeometryN(1)));
    }

    @Test
    public void affine2DGeom3D() {
        WKTWriter wktWriter = new WKTWriter(3);
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 1, 1, 1, 2, 1, 2, 2));
        String expected = wktWriter.write(GEOMETRY_FACTORY.createLineString(coordArray3d(6, 8, 1, 7, 11, 2, 8, 14, 2)));
        String actual = wktWriter.write(Functions.affine(lineString, 1d, 1d, 2d, 3d, 5d, 6d));
        assertEquals(expected, actual);
    }


    @Test
    public void affine2DGeom2D() {
        LineString lineString = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 1, 2));
        String expected = GEOMETRY_FACTORY.createLineString(coordArray(6, 8, 7, 11, 8, 14)).toText();
        String actual = Functions.affine(lineString, 1.0, 1.0, 2.0, 3.0, 5.0, 6.0).toText();
        assertEquals(expected, actual);
    }

    @Test
    public void affine2DHybridGeomCollection() {
        Point point3D = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        Polygon polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        Polygon polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(3, 4, 3, 5, 3, 7, 10, 7, 3, 4));
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[] {polygon1, polygon2});
        Geometry geomCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {point3D, multiPolygon})});
        Geometry actualGeomCollection = Functions.affine(geomCollection, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0);
        Point expectedPoint3D = GEOMETRY_FACTORY.createPoint(new Coordinate(4, 5));
        Polygon expectedPolygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(2, 3, 4, 5, 5, 6, 3, 4, 2, 3));
        Polygon expectedPolygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(12, 13, 14, 15, 18, 19, 25, 26, 12, 13));
        assertEquals(expectedPoint3D.toText(), actualGeomCollection.getGeometryN(0).getGeometryN(0).toText());
        assertEquals(expectedPolygon1.toText(), actualGeomCollection.getGeometryN(0).getGeometryN(1).getGeometryN(0).toText());
        assertEquals(expectedPolygon2.toText(), actualGeomCollection.getGeometryN(0).getGeometryN(1).getGeometryN(1).toText());
    }

    @Test
    public void geometryTypeWithMeasured2D() {
        String expected1 = "POINT";
        String actual1 = Functions.geometryTypeWithMeasured(GEOMETRY_FACTORY.createPoint(new Coordinate(10, 5)));
        assertEquals(expected1, actual1);

        // Create a point with measure value
        CoordinateXYM coords = new CoordinateXYM(2, 3, 4);
        Point measuredPoint = GEOMETRY_FACTORY.createPoint(coords);
        String expected2 = "POINTM";
        String actual2 = Functions.geometryTypeWithMeasured(measuredPoint);
        assertEquals(expected2, actual2);

        // Create a linestring with measure value
        CoordinateXYM[] coordsLineString = new CoordinateXYM[] {new CoordinateXYM(1, 2, 3), new CoordinateXYM(4, 5, 6)};
        LineString measuredLineString = GEOMETRY_FACTORY.createLineString(coordsLineString);
        String expected3 = "LINESTRINGM";
        String actual3 = Functions.geometryTypeWithMeasured(measuredLineString);
        assertEquals(expected3, actual3);

        // Create a polygon with measure value
        CoordinateXYM[] coordsPolygon = new CoordinateXYM[] {new CoordinateXYM(0, 0, 0), new CoordinateXYM(1, 1, 0), new CoordinateXYM(0, 1, 0), new CoordinateXYM(0, 0, 0)};
        Polygon measuredPolygon = GEOMETRY_FACTORY.createPolygon(coordsPolygon);
        String expected4 = "POLYGONM";
        String actual4 = Functions.geometryTypeWithMeasured(measuredPolygon);
        assertEquals(expected4, actual4);
    }

    @Test
    public void geometryTypeWithMeasured3D() {
        String expected1 = "POINT";
        String actual1 = Functions.geometryTypeWithMeasured(GEOMETRY_FACTORY.createPoint(new Coordinate(10, 5, 1)));
        assertEquals(expected1, actual1);

        // Create a point with measure value
        CoordinateXYZM coordsPoint = new CoordinateXYZM(2, 3, 4, 0);
        Point measuredPoint = GEOMETRY_FACTORY.createPoint(coordsPoint);
        String expected2 = "POINTM";
        String actual2 = Functions.geometryTypeWithMeasured(measuredPoint);
        assertEquals(expected2, actual2);

        // Create a linestring with measure value
        CoordinateXYZM[] coordsLineString = new CoordinateXYZM[] {new CoordinateXYZM(1, 2, 3, 0), new CoordinateXYZM(4, 5, 6, 0)};
        LineString measuredLineString = GEOMETRY_FACTORY.createLineString(coordsLineString);
        String expected3 = "LINESTRINGM";
        String actual3 = Functions.geometryTypeWithMeasured(measuredLineString);
        assertEquals(expected3, actual3);

        // Create a polygon with measure value
        CoordinateXYZM[] coordsPolygon = new CoordinateXYZM[] {new CoordinateXYZM(0, 0, 0, 0), new CoordinateXYZM(1, 1, 0, 0), new CoordinateXYZM(0, 1, 0, 0), new CoordinateXYZM(0, 0, 0, 0)};
        Polygon measuredPolygon = GEOMETRY_FACTORY.createPolygon(coordsPolygon);
        String expected4 = "POLYGONM";
        String actual4 = Functions.geometryTypeWithMeasured(measuredPolygon);
        assertEquals(expected4, actual4);
    }

    @Test
    public void geometryTypeWithMeasuredCollection() {
        String expected1 = "GEOMETRYCOLLECTION";
        String actual1 = Functions.geometryTypeWithMeasured(GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {GEOMETRY_FACTORY.createPoint(new Coordinate(10, 5))}));
        assertEquals(expected1, actual1);

        // Create a geometrycollection with measure value
        CoordinateXYM coords = new CoordinateXYM(2, 3, 4);
        Point measuredPoint = GEOMETRY_FACTORY.createPoint(coords);
        String expected2 = "GEOMETRYCOLLECTIONM";
        String actual2 = Functions.geometryTypeWithMeasured(GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {measuredPoint}));
        assertEquals(expected2, actual2);

        // Create a geometrycollection with measure value
        CoordinateXYM[] coordsLineString = new CoordinateXYM[] {new CoordinateXYM(1, 2, 3), new CoordinateXYM(4, 5, 6)};
        LineString measuredLineString = GEOMETRY_FACTORY.createLineString(coordsLineString);
        String expected3 = "GEOMETRYCOLLECTIONM";
        String actual3 = Functions.geometryTypeWithMeasured(GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {measuredLineString}));
        assertEquals(expected3, actual3);

        // Create a geometrycollection with measure value
        CoordinateXYM[] coordsPolygon = new CoordinateXYM[] {new CoordinateXYM(0, 0, 0), new CoordinateXYM(1, 1, 0), new CoordinateXYM(0, 1, 0), new CoordinateXYM(0, 0, 0)};
        Polygon measuredPolygon = GEOMETRY_FACTORY.createPolygon(coordsPolygon);
        String expected4 = "GEOMETRYCOLLECTIONM";
        String actual4 = Functions.geometryTypeWithMeasured(GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {measuredPolygon}));
        assertEquals(expected4, actual4);
    }

    @Test
    public void closestPoint() {
        Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        LineString lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        String expected1 = "POINT (1 1)";
        String actual1 = Functions.closestPoint(point1, lineString1).toText();
        assertEquals(expected1, actual1);

        Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(160, 40));
        LineString lineString2 = GEOMETRY_FACTORY.createLineString(coordArray(10, 30, 50, 50, 30, 110, 70, 90, 180, 140, 130, 190));
        String expected2 = "POINT (160 40)";
        String actual2 = Functions.closestPoint(point2, lineString2).toText();
        assertEquals(expected2, actual2);
        Point expectedPoint3 = GEOMETRY_FACTORY.createPoint(new Coordinate(125.75342465753425, 115.34246575342466));
        Double expected3 = Functions.closestPoint(lineString2, point2).distance(expectedPoint3);
        assertEquals(expected3, 0, 1e-6);

        Point point4 = GEOMETRY_FACTORY.createPoint(new Coordinate(80, 160));
        Polygon polygonA = GEOMETRY_FACTORY.createPolygon(coordArray(190, 150, 20, 10, 160, 70, 190, 150));
        Geometry polygonB = Functions.buffer(point4, 30);
        Point expectedPoint4 = GEOMETRY_FACTORY.createPoint(new Coordinate(131.59149149528952, 101.89887534906197));
        Double expected4 = Functions.closestPoint(polygonA, polygonB).distance(expectedPoint4);
        assertEquals(expected4, 0, 1e-6);
    }

    @Test
    public void closestPoint3d() {
        // One of the object is 3D
        Point point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1,10000));
        LineString lineString1 = GEOMETRY_FACTORY.createLineString(coordArray(1, 0, 1, 1, 2, 1, 2, 0, 1, 0));
        String expected1 = "POINT (1 1)";
        String actual1 = Functions.closestPoint(point1, lineString1).toText();
        assertEquals(expected1, actual1);

        // Both of the object are 3D
        LineString lineString3D = GEOMETRY_FACTORY.createLineString(coordArray3d(1, 0, 100, 1, 1, 20, 2, 1, 40, 2, 0, 60, 1, 0, 70));
        String expected2 = "POINT (1 1)";
        String actual2 = Functions.closestPoint(point1, lineString3D).toText();
        assertEquals(expected2, actual2);
    }

    @Test
    public void closestPointGeomtryCollection() {
        LineString line = GEOMETRY_FACTORY.createLineString(coordArray(2, 0, 0, 2));
        Geometry[] geometry = new Geometry[] {
                GEOMETRY_FACTORY.createLineString(coordArray(2, 0, 2, 1)),
                GEOMETRY_FACTORY.createPolygon(coordArray(0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0))
        };
        GeometryCollection geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(geometry);
        String actual1 = Functions.closestPoint(line, geometryCollection).toText();
        String expected1 = "POINT (2 0)";
        assertEquals(actual1 ,expected1);
    }

    @Test
    public void closestPointEmpty() {
        // One of the object is empty
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        LineString emptyLineString = GEOMETRY_FACTORY.createLineString();
        String expected = "ST_ClosestPoint doesn't support empty geometry object.";
        Exception e1 = assertThrows(IllegalArgumentException.class, () -> Functions.closestPoint(point, emptyLineString));
        assertEquals(expected, e1.getMessage());

        // Both objects are empty
        Polygon emptyPolygon = GEOMETRY_FACTORY.createPolygon();
        Exception e2 = assertThrows(IllegalArgumentException.class, () -> Functions.closestPoint(emptyPolygon, emptyLineString));
        assertEquals(expected, e2.getMessage());
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

    @Test
    public void transform()
            throws FactoryException, TransformException
    {
        // The source and target CRS are the same
        Point geomExpected = GEOMETRY_FACTORY.createPoint(new Coordinate(120, 60));
        Geometry geomActual = FunctionsGeoTools.transform(geomExpected, "EPSG:4326", "EPSG:4326");
        assertEquals(geomExpected.getCoordinate().x, geomActual.getCoordinate().x, FP_TOLERANCE);
        assertEquals(geomExpected.getCoordinate().y, geomActual.getCoordinate().y, FP_TOLERANCE);

        // The source and target CRS are different
        geomActual = FunctionsGeoTools.transform(geomExpected, "EPSG:4326", "EPSG:3857");
        assertEquals(1.3358338895192828E7, geomActual.getCoordinate().x, FP_TOLERANCE);
        assertEquals(8399737.889818355, geomActual.getCoordinate().y, FP_TOLERANCE);

        // The source CRS is not specified and the geometry has no SRID
        Exception e = assertThrows(IllegalArgumentException.class, () -> FunctionsGeoTools.transform(geomExpected,"EPSG:3857"));
        assertEquals("Source CRS must be specified. No SRID found on geometry.", e.getMessage());

        // The source CRS is an invalid SRID
        e = assertThrows(FactoryException.class, () -> FunctionsGeoTools.transform(geomExpected, "abcde", "EPSG:3857"));
        assertTrue(e.getMessage().contains("First failed to read as a well-known CRS code"));

        // The source CRS is a WKT CRS string
        String crsWkt = CRS.decode("EPSG:4326", true).toWKT();
        geomActual = FunctionsGeoTools.transform(geomExpected, crsWkt, "EPSG:3857");
        assertEquals(1.3358338895192828E7, geomActual.getCoordinate().x, FP_TOLERANCE);
        assertEquals(8399737.889818355, geomActual.getCoordinate().y, FP_TOLERANCE);

        // The source CRS is not specified but the geometry has a valid SRID
        geomExpected.setSRID(4326);
        geomActual = FunctionsGeoTools.transform(geomExpected, "EPSG:3857");
        assertEquals(1.3358338895192828E7, geomActual.getCoordinate().x, FP_TOLERANCE);
        assertEquals(8399737.889818355, geomActual.getCoordinate().y, FP_TOLERANCE);

        // The source and target CRS are different, and latitude is out of range
        Point geometryWrong = GEOMETRY_FACTORY.createPoint(new Coordinate(60, 120));
        assertThrows(ProjectionException.class, () -> FunctionsGeoTools.transform(geometryWrong, "EPSG:4326", "EPSG:3857"));


    }

    @Test
    public void voronoiPolygons() {
        MultiPoint multiPoint = GEOMETRY_FACTORY.createMultiPointFromCoords(coordArray(0, 0, 2, 2));
        Geometry actual1 = FunctionsGeoTools.voronoiPolygons(multiPoint, 0, null);
        assertEquals("GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 -2, -2 -2)), POLYGON ((-2 4, 4 4, 4 -2, -2 4)))",
                actual1.toText());

        Geometry actual2 = FunctionsGeoTools.voronoiPolygons(multiPoint, 30, null);
        assertEquals("GEOMETRYCOLLECTION (POLYGON ((-2 -2, -2 4, 4 4, 4 -2, -2 -2)))", actual2.toText());

        Geometry buf = Functions.buffer(GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1)), 10);
        Geometry actual3 = FunctionsGeoTools.voronoiPolygons(multiPoint, 0, buf);
        assertEquals(
                "GEOMETRYCOLLECTION (POLYGON ((-9 -9, -9 11, 11 -9, -9 -9)), POLYGON ((-9 11, 11 11, 11 -9, -9 11)))",
                actual3.toText());

        Geometry actual4 = FunctionsGeoTools.voronoiPolygons(multiPoint, 30, buf);
        assertEquals("GEOMETRYCOLLECTION (POLYGON ((-9 -9, -9 11, 11 11, 11 -9, -9 -9)))", actual4.toText());

        Geometry actual5 = FunctionsGeoTools.voronoiPolygons(null, 0, null);
        assertEquals(null, actual5);
    }
}
