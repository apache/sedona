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
package org.apache.sedona.flink;

import org.apache.commons.codec.binary.Hex;
import org.apache.flink.table.api.Table;
import org.apache.sedona.flink.expressions.Functions;
import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import static junit.framework.TestCase.assertNull;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FunctionTest extends TestBase{
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize();
    }

    @Test
    public void testArea() {
        Table polygonTable = createPolygonTable(1);
        Table ResultTable = polygonTable.select(call(Functions.ST_Area.class.getSimpleName(), $(polygonColNames[0])));
        assertNotNull(first(ResultTable).getField(0));
        double result = (double) first(ResultTable).getField(0);
        assertEquals(1.0, result, 0);
    }

    @Test
    public void testAzimuth() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_Azimuth(ST_GeomFromWKT('POINT (0 0)'), ST_GeomFromWKT('POINT (1 1)'))");
        assertEquals(45, ((double) first(pointTable).getField(0)) / (Math.PI * 2) * 360, 0);
    }

    @Test
    public void testBoundary() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POLYGON ((1 1, 0 0, -1 1, 1 1))') AS geom");
        Table boundaryTable = polygonTable.select(call(Functions.ST_Boundary.class.getSimpleName(), $("geom")));
        Geometry result = (Geometry) first(boundaryTable).getField(0);
        assertEquals("LINEARRING (1 1, 0 0, -1 1, 1 1)", result.toString());
    }

    @Test
    public void testBuffer() {
        Table pointTable = createPointTable_real(testDataSize);
        Table bufferTable = pointTable.select(call(Functions.ST_Buffer.class.getSimpleName(), $(pointColNames[0]), 1));
        Geometry result = (Geometry) first(bufferTable).getField(0);
        assert(result instanceof Polygon);
    }

    @Test
    public void testConcaveHull() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))') as geom");
        Table concaveHullPolygonTable = polygonTable.select(call(Functions.ST_ConcaveHull.class.getSimpleName(), $("geom"), 1.0, true));
        Geometry result = (Geometry) first(concaveHullPolygonTable).getField(0);
        assertEquals("POLYGON ((1 2, 2 2, 3 2, 5 0, 4 0, 1 0, 0 0, 1 2))", result.toString());

        Table polygonTable2 = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))') as geom");
        Table concaveHullPolygonTable2 = polygonTable2.select(call(Functions.ST_ConcaveHull.class.getSimpleName(), $("geom"), 1.0));
        Geometry result2 = (Geometry) first(concaveHullPolygonTable2).getField(0);
        assertEquals("POLYGON ((0 0, 1 1, 1 0, 0 0))", result2.toString());
    }

    @Test
    public void testEnvelope() {
        Table linestringTable = createLineStringTable(1);
        linestringTable = linestringTable.select(call(Functions.ST_Envelope.class.getSimpleName(), $(linestringColNames[0])));
        assertEquals("POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", first(linestringTable).getField(0).toString());
    }

    @Test
    public void testFlipCoordinates() {
        Table pointTable = createPointTable_real(testDataSize);
        Table flippedTable = pointTable.select(call(Functions.ST_FlipCoordinates.class.getSimpleName(), $(pointColNames[0])));
        Geometry result = (Geometry) first(flippedTable).getField(0);
        assertEquals("POINT (-117.99 32.01)", result.toString());
    }

    @Test
    public void testTransform() {
        Table pointTable = createPointTable_real(testDataSize);
        Table transformedTable = pointTable.select(call(Functions.ST_Transform.class.getSimpleName(), $(pointColNames[0])
                , "epsg:4326", "epsg:3857"));
        String result = first(transformedTable).getField(0).toString();
        assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result);
    }

    @Test
    public void testTransformWKT() throws FactoryException {
        Table pointTable = createPointTable_real(testDataSize);

        CoordinateReferenceSystem CRS_SRC = CRS.decode("epsg:4326");
        CoordinateReferenceSystem CRS_TGT = CRS.decode("epsg:3857");

        String SRC_WKT = CRS_SRC.toWKT();
        String TGT_WKT = CRS_TGT.toWKT();

        Table transformedTable_SRC = pointTable.select(call(Functions.ST_Transform.class.getSimpleName(), $(pointColNames[0])
                , SRC_WKT, "epsg:3857"));
        String result_SRC = first(transformedTable_SRC).getField(0).toString();
        assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result_SRC);

        Table transformedTable_TGT = pointTable.select(call(Functions.ST_Transform.class.getSimpleName(), $(pointColNames[0])
                , "epsg:4326", TGT_WKT));
        String result_TGT = first(transformedTable_TGT).getField(0).toString();
        assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result_TGT);

        Table transformedTable_SRC_TGT = pointTable.select(call(Functions.ST_Transform.class.getSimpleName(), $(pointColNames[0])
                , SRC_WKT, TGT_WKT));
        String result_SRC_TGT = first(transformedTable_SRC_TGT).getField(0).toString();
        assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result_SRC_TGT);

        Table transformedTable_SRC_TGT_lenient = pointTable.select(call(Functions.ST_Transform.class.getSimpleName(), $(pointColNames[0])
                , SRC_WKT, TGT_WKT,false));
        String result_SRC_TGT_lenient = first(transformedTable_SRC_TGT_lenient).getField(0).toString();
        assertEquals("POINT (-13134586.718698347 3764623.3541299687)", result_SRC_TGT_lenient);

    }


    @Test
    public void testDistance() {
        Table pointTable = createPointTable(testDataSize);
        pointTable = pointTable.select(call(Functions.ST_Distance.class.getSimpleName(), $(pointColNames[0])
                , call("ST_GeomFromWKT", "POINT (0 0)")));
        assertEquals(0.0, first(pointTable).getField(0));
    }

    @Test
    public void test3dDistance() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_3DDistance(ST_GeomFromWKT('POINT (0 0 0)'), ST_GeomFromWKT('POINT (1 1 1)'))");
        assertEquals(Math.sqrt(3), first(pointTable).getField(0));
    }

    @Test
    public void testLength() {
        Table polygonTable = createPolygonTable(1);
        Table resultTable = polygonTable.select(call(Functions.ST_Length.class.getSimpleName(), $(polygonColNames[0])));
        assertNotNull(first(resultTable).getField(0));
        double result = (double) first(resultTable).getField(0);
        assertEquals(4, result, 0);
    }

    @Test
    public void testYMax() {
        Table polygonTable = createPolygonTable(1);
        Table ResultTable = polygonTable.select(call(Functions.ST_YMax.class.getSimpleName(), $(polygonColNames[0])));
        assertNotNull(first(ResultTable).getField(0));
        double result = (double) first(ResultTable).getField(0);
        assertEquals(0.5, result,0);
    }

    @Test
    public void testYMin() {
        Table polygonTable = createPolygonTable(1);
        Table ResultTable = polygonTable.select(call(Functions.ST_YMin.class.getSimpleName(), $(polygonColNames[0])));
        assertNotNull(first(ResultTable).getField(0));
        double result = (double) first(ResultTable).getField(0);
        assertEquals(-0.5, result, 0);
    }

    @Test
    public void testGeomToGeoHash() {
        Table pointTable = createPointTable(testDataSize);
        pointTable = pointTable.select(
                call("ST_GeoHash", $(pointColNames[0]), 5)
        );
        assertEquals(first(pointTable).getField(0), "s0000");
    }

    @Test
    public void testPointOnSurface() {
        Table pointTable = createPointTable_real(testDataSize);
        Table surfaceTable = pointTable.select(call(Functions.ST_PointOnSurface.class.getSimpleName(), $(pointColNames[0])));
        Geometry result = (Geometry) first(surfaceTable).getField(0);
        assertEquals("POINT (32.01 -117.99)", result.toString());
    }

    @Test
    public void testReverse() {
        Table polygonTable = createPolygonTable(1);
        Table ReversedTable = polygonTable.select(call(Functions.ST_Reverse.class.getSimpleName(), $(polygonColNames[0])));
        Geometry result = (Geometry) first(ReversedTable).getField(0);
        assertEquals("POLYGON ((-0.5 -0.5, 0.5 -0.5, 0.5 0.5, -0.5 0.5, -0.5 -0.5))", result.toString());
    }

    @Test
    public void testGeometryN() {
        Table collectionTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))') AS collection");
        Table resultTable = collectionTable.select(call(Functions.ST_GeometryN.class.getSimpleName(), $("collection"), 1));
        Point point = (Point) first(resultTable).getField(0);
        assertEquals("POINT (30 30)", point.toString());
    }

    @Test
    public void testInteriorRingN() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromText('POLYGON((7 9,8 7,11 6,15 8,16 6,17 7,17 10,18 12,17 14,15 15,11 15,10 13,9 12,7 9),(9 9,10 10,11 11,11 10,10 8,9 9),(12 14,15 14,13 11,12 14))') AS polygon");
        Table resultTable = polygonTable.select(call(Functions.ST_InteriorRingN.class.getSimpleName(), $("polygon"), 1));
        LinearRing linearRing = (LinearRing) first(resultTable).getField(0);
        assertEquals("LINEARRING (12 14, 15 14, 13 11, 12 14)", linearRing.toString());
    }

    @Test
    public void testPointN_positiveN() {
        int n = 1;
        Table polygonTable = createPolygonTable(1);
        Table linestringTable = polygonTable.select(call(Functions.ST_ExteriorRing.class.getSimpleName(), $(polygonColNames[0])));
        Table pointTable = linestringTable.select(call(Functions.ST_PointN.class.getSimpleName(), $("_c0"), n));
        Point point = (Point) first(pointTable).getField(0);
        assertNotNull(point);
        assertEquals("POINT (-0.5 -0.5)", point.toString());
    }

    @Test
    public void testPointN_negativeN() {
        int n = -3;
        Table polygonTable = createPolygonTable(1);
        Table linestringTable = polygonTable.select(call(Functions.ST_ExteriorRing.class.getSimpleName(), $(polygonColNames[0])));
        Table pointTable = linestringTable.select(call(Functions.ST_PointN.class.getSimpleName(), $("_c0"), n));
        Point point = (Point) first(pointTable).getField(0);
        assertNotNull(point);
        assertEquals("POINT (0.5 0.5)", point.toString());
    }

    @Test
    public void testNPoints() {
        Table polygonTable = createPolygonTable(1);
        Table resultTable = polygonTable.select(call(Functions.ST_NPoints.class.getSimpleName(), $(polygonColNames[0])));
        assertEquals(5, first(resultTable).getField(0));
    }

    @Test
    public void testNumGeometries() {
        Table collectionTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))') AS collection");
        Table resultTable = collectionTable.select(call(Functions.ST_NumGeometries.class.getSimpleName(), $("collection")));
        assertEquals(3, first(resultTable).getField(0));
    }

    @Test
    public void testNumInteriorRings() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromText('POLYGON((7 9,8 7,11 6,15 8,16 6,17 7,17 10,18 12,17 14,15 15,11 15,10 13,9 12,7 9),(9 9,10 10,11 11,11 10,10 8,9 9),(12 14,15 14,13 11,12 14))') AS polygon");
        Table resultTable = polygonTable.select(call(Functions.ST_NumInteriorRings.class.getSimpleName(), $("polygon")));
        assertEquals(2, first(resultTable).getField(0));
    }

    @Test
    public void testExteriorRing() {
        Table polygonTable = createPolygonTable(1);
        Table linearRingTable = polygonTable.select(call(Functions.ST_ExteriorRing.class.getSimpleName(), $(polygonColNames[0])));
        LinearRing linearRing = (LinearRing) first(linearRingTable).getField(0);
        assertNotNull(linearRing);
        Assert.assertEquals("LINEARRING (-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5)", linearRing.toString());
    }

    @Test
    public void testAsEWKT() {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_AsEWKT.class.getSimpleName(), $(polygonColNames[0])));
        String result = (String) first(polygonTable).getField(0);
        assertEquals("POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result);
    }

    @Test
    public void testAsText() {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_AsText.class.getSimpleName(), $(polygonColNames[0])));
        String result = (String) first(polygonTable).getField(0);
        assertEquals("POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result);
    }

    @Test
    public void testAsEWKB() {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_AsEWKB.class.getSimpleName(), $(polygonColNames[0])));
        String result = Hex.encodeHexString((byte[]) first(polygonTable).getField(0));
        assertEquals("01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e0bf000000000000e03f000000000000e03f000000000000e03f000000000000e03f000000000000e0bf000000000000e0bf000000000000e0bf", result);
    }

    @Test
    public void testAsBinary() {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_AsBinary.class.getSimpleName(), $(polygonColNames[0])));
        String result = Hex.encodeHexString((byte[]) first(polygonTable).getField(0));
        assertEquals("01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e0bf000000000000e03f000000000000e03f000000000000e03f000000000000e03f000000000000e0bf000000000000e0bf000000000000e0bf", result);
    }

    @Test
    public void testAsGML() throws Exception {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_AsGML.class.getSimpleName(), $(polygonColNames[0])));
        String result = (String) first(polygonTable).getField(0);
        String expected =
                "<gml:Polygon>\n" +
                "  <gml:outerBoundaryIs>\n" +
                "    <gml:LinearRing>\n" +
                "      <gml:coordinates>\n" +
                "        -0.5,-0.5 -0.5,0.5 0.5,0.5 0.5,-0.5 -0.5,-0.5 \n" +
                "      </gml:coordinates>\n" +
                "    </gml:LinearRing>\n" +
                "  </gml:outerBoundaryIs>\n" +
                "</gml:Polygon>\n";
        assertEquals(expected, result);
    }

    @Test
    public void testAsKML() {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_AsKML.class.getSimpleName(), $(polygonColNames[0])));
        String result = (String) first(polygonTable).getField(0);
        String expected =
                "<Polygon>\n" +
                "  <outerBoundaryIs>\n" +
                "  <LinearRing>\n" +
                "    <coordinates>-0.5,-0.5 -0.5,0.5 0.5,0.5 0.5,-0.5 -0.5,-0.5</coordinates>\n" +
                "  </LinearRing>\n" +
                "  </outerBoundaryIs>\n" +
                "</Polygon>\n";
        assertEquals(expected, result);
    }

    @Test
    public void testGeoJSON() {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_AsGeoJSON.class.getSimpleName(), $(polygonColNames[0])));
        String result = (String) first(polygonTable).getField(0);
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[-0.5,-0.5],[-0.5,0.5],[0.5,0.5],[0.5,-0.5],[-0.5,-0.5]]]}", result);
    }

    @Test
    public void testForce2D() {
        Table polygonTable = createPolygonTable(1);
        Table Forced2DTable = polygonTable.select(call(Functions.ST_Force_2D.class.getSimpleName(), $(polygonColNames[0])));
        Geometry result = (Geometry) first(Forced2DTable).getField(0);
        assertEquals("POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result.toString());
    }

    @Test
    public void testIsEmpty() {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_IsEmpty.class.getSimpleName(), $(polygonColNames[0])));
        boolean result = (boolean) first(polygonTable).getField(0);
        assertEquals(false, result);
    }

    @Test
    public void testX() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (1.23 4.56 7.89)') AS " + pointColNames[0]);
        pointTable = pointTable.select(call(Functions.ST_X.class.getSimpleName(), $(pointColNames[0])));
        assertEquals(1.23, first(pointTable).getField(0));
    }

    @Test
    public void testY() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (1.23 4.56 7.89)') AS " + pointColNames[0]);
        pointTable = pointTable.select(call(Functions.ST_Y.class.getSimpleName(), $(pointColNames[0])));
        assertEquals(4.56, first(pointTable).getField(0));
    }

    @Test
    public void testZ() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (1.23 4.56 7.89)') AS " + pointColNames[0]);
        pointTable = pointTable.select(call(Functions.ST_Z.class.getSimpleName(), $(pointColNames[0])));
        assertEquals(7.89, first(pointTable).getField(0));
    }

    @Test
    public void testZMax() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 3 4, 5 6 7)') AS " + polygonColNames[0]);
        polygonTable = polygonTable.select(call(Functions.ST_ZMax.class.getSimpleName(), $(polygonColNames[0])));
        double result = (double) first(polygonTable).getField(0);
        assertEquals(7.0, result, 0);
    }

    @Test
    public void testZMaxWithNoZCoordinate() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 3, 5 6)') AS " + polygonColNames[0]);
        polygonTable = polygonTable.select(call(Functions.ST_ZMax.class.getSimpleName(), $(polygonColNames[0])));
        assertNull(first(polygonTable).getField(0));
    }

    @Test
    public void testZMin() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 3 4, 5 6 7)') AS " + polygonColNames[0]);
        polygonTable = polygonTable.select(call(Functions.ST_ZMin.class.getSimpleName(), $(polygonColNames[0])));
        double result = (double) first(polygonTable).getField(0);
        assertEquals(4.0, result, 0);
    }

    @Test
    public void testZMinWithNoZCoordinate() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING(1 3, 5 6)') AS " + polygonColNames[0]);
        polygonTable = polygonTable.select(call(Functions.ST_ZMin.class.getSimpleName(), $(polygonColNames[0])));
        assertNull(first(polygonTable).getField(0));
    }

    @Test
    public void testNDimsFor2D() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT(1 1)') AS " + polygonColNames[0]);
        polygonTable = polygonTable.select(call(Functions.ST_NDims.class.getSimpleName(), $(polygonColNames[0])));
        int result = (int) first(polygonTable).getField(0);
        assertEquals(2, result, 0);
    }

    @Test
    public void testNDims() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT(1 1 2)') AS " + polygonColNames[0]);
        polygonTable = polygonTable.select(call(Functions.ST_NDims.class.getSimpleName(), $(polygonColNames[0])));
        int result = (int) first(polygonTable).getField(0);
        assertEquals(3, result, 0);
    }

    @Test
    public void testNDimsForMCoordinate() {
        Object result = first(tableEnv.sqlQuery("SELECT ST_NDims(ST_GeomFromWKT('POINT M (1 2 3)'))")).getField(0);
        assertEquals(result, 3);
        result = first(tableEnv.sqlQuery("SELECT ST_NDims(ST_GeomFromWKT('POINT ZM (1 2 3 4)'))")).getField(0);
        assertEquals(result, 4);
    }

    @Test
    public void testXMax() {
        Table polygonTable = createPolygonTable(1);
        Table MaxTable = polygonTable.select(call(Functions.ST_XMax.class.getSimpleName(), $(polygonColNames[0])));
        double result = (double) first(MaxTable).getField(0);
        assertEquals(0.5, result,0);
    }

    @Test
    public void testXMin() {
        Table polygonTable = createPolygonTable(1);
        Table MinTable = polygonTable.select(call(Functions.ST_XMin.class.getSimpleName(), $(polygonColNames[0])));
        double result = (double) first(MinTable).getField(0);
        assertEquals(-0.5, result,0);
    }

    @Test
    public void testBuildArea() {
        Table polygonTable = createPolygonTable(1);
        Table arealGeomTable = polygonTable.select(call(Functions.ST_BuildArea.class.getSimpleName(), $(polygonColNames[0])));
        Geometry result = (Geometry) first(arealGeomTable).getField(0);
        assertEquals("POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result.toString());
    }

    @Test
    public void testSetSRID() {
        Table polygonTable = createPolygonTable(1);
        polygonTable = polygonTable
                .select(call(Functions.ST_SetSRID.class.getSimpleName(), $(polygonColNames[0]), 3021))
                .select(call(Functions.ST_SRID.class.getSimpleName(), $("_c0")));
        int result = (int) first(polygonTable).getField(0);
        assertEquals(3021, result);
    }

    @Test
    public void testSRID() {
        Table polygonTable = createPolygonTable(1);
        polygonTable = polygonTable.select(call(Functions.ST_SRID.class.getSimpleName(), $(polygonColNames[0])));
        int result = (int) first(polygonTable).getField(0);
        assertEquals(0, result);
    }

    @Test
    public void testIsClosedForOpen() {
        Table linestringTable = createLineStringTable(1);
        linestringTable = linestringTable.select(call(Functions.ST_IsClosed.class.getSimpleName(), $(linestringColNames[0])));
        assertFalse((boolean) first(linestringTable).getField(0));
    }

    @Test
    public void testIsClosedForClosed() {
        Table polygonTable = createPolygonTable(1);
        polygonTable = polygonTable.select(call(Functions.ST_IsClosed.class.getSimpleName(), $(polygonColNames[0])));
        assertTrue((boolean) first(polygonTable).getField(0));
    }

    @Test
    public void testIsRingForRing() {
        Table polygonTable = createPolygonTable(1);
        Table linestringTable = polygonTable.select(call(Functions.ST_ExteriorRing.class.getSimpleName(), $(polygonColNames[0])));
        linestringTable = linestringTable.select(call(Functions.ST_IsRing.class.getSimpleName(), $("_c0")));
        assertTrue((boolean) first(linestringTable).getField(0));
    }

    @Test
    public void testIsRingForNonRing() {
        Table linestringTable = createLineStringTable(1);
        linestringTable = linestringTable.select(call(Functions.ST_IsClosed.class.getSimpleName(), $(linestringColNames[0])));
        assertFalse((boolean) first(linestringTable).getField(0));
    }

    @Test
    public void testIsSimple() {
        Table polygonTable = createPolygonTable(1);
        polygonTable = polygonTable.select(call(Functions.ST_IsSimple.class.getSimpleName(), $(polygonColNames[0])));
        assertTrue((boolean) first(polygonTable).getField(0));
    }

    @Test
    public void testIsValid() {
        Table polygonTable = createPolygonTable(1);
        polygonTable = polygonTable.select(call(Functions.ST_IsValid.class.getSimpleName(), $(polygonColNames[0])));
        assertTrue((boolean) first(polygonTable).getField(0));
    }

    @Test
    public void testNormalize() {
        Table polygonTable = tableEnv.sqlQuery("SELECT ST_GeomFromText('POLYGON((0 1, 1 1, 1 0, 0 0, 0 1))') AS polygon");
        polygonTable = polygonTable.select(call(Functions.ST_Normalize.class.getSimpleName(), $("polygon")));
        Geometry result = (Geometry) first(polygonTable).getField(0);
        assertEquals("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", result.toString());
    }

    @Test
    public void testAddPoint() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_AddPoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1)'), ST_GeomFromWKT('POINT (2 2)'))");
        assertEquals("LINESTRING (0 0, 1 1, 2 2)", first(pointTable).getField(0).toString());
    }

    @Test
    public void testAddPointWithIndex() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_AddPoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1)'), ST_GeomFromWKT('POINT (2 2)'), 1)");
        assertEquals("LINESTRING (0 0, 2 2, 1 1)", first(pointTable).getField(0).toString());
    }

    @Test
    public void testRemovePoint() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_RemovePoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'))");
        assertEquals("LINESTRING (0 0, 1 1)", first(pointTable).getField(0).toString());
    }

    @Test
    public void testRemovePointWithIndex() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_RemovePoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'), 1)");
        assertEquals("LINESTRING (0 0, 2 2)", first(pointTable).getField(0).toString());
    }

    @Test
    public void testSetPoint() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_SetPoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'), 0, ST_GeomFromWKT('POINT (3 3)'))");
        assertEquals("LINESTRING (3 3, 1 1, 2 2)", first(pointTable).getField(0).toString());
    }

    @Test
    public void testSetPointWithNegativeIndex() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_SetPoint(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'), -1, ST_GeomFromWKT('POINT (3 3)'))");
        assertEquals("LINESTRING (0 0, 1 1, 3 3)", first(pointTable).getField(0).toString());
    }

    @Test
    public void testLineFromMultiPoint() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_LineFromMultiPoint(ST_GeomFromWKT('MULTIPOINT((10 40), (40 30), (20 20), (30 10))'))");
        assertEquals("LINESTRING (10 40, 40 30, 20 20, 30 10)", first(pointTable).getField(0).toString());
    }

    @Test
    public void testSplit() {
        Table pointTable = tableEnv.sqlQuery("SELECT ST_Split(ST_GeomFromWKT('LINESTRING (0 0, 1.5 1.5, 2 2)'), ST_GeomFromWKT('MULTIPOINT (0.5 0.5, 1 1)'))");
        assertEquals("MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))", ((Geometry)first(pointTable).getField(0)).norm().toText());
    }
}
