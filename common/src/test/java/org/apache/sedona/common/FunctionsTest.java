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

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

import static org.junit.Assert.*;

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
}
