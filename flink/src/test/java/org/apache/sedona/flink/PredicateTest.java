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

import org.apache.calcite.runtime.Geometries;
import org.apache.flink.table.api.Table;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.flink.expressions.Predicates;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import static org.junit.Assert.assertEquals;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.Assert.assertThrows;

public class PredicateTest extends TestBase{

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize();
    }

    @Test
    public void testIntersects() {
        Table pointTable = createPointTable(testDataSize);
        String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
        String expr = "ST_Intersects(ST_GeomFromWkt('" + polygon + "'), geom_point)";
        Table result = pointTable.filter(expr);
        assertEquals(1, count(result));
    }

    @Test
    public void testDisjoint() {
        Table pointTable = createPointTable(testDataSize);
        String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
        String expr = "ST_Disjoint(ST_GeomFromWkt('" + polygon + "'), geom_point)";
        Table result = pointTable.filter(expr);
        assertEquals(999, count(result));
    }

    @Test
    public void testContains() {
        Table pointTable = createPointTable(testDataSize);
        String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
        String expr = "ST_Contains(ST_GeomFromWkt('" + polygon + "'), geom_point)";
        Table result = pointTable.filter(expr);
        assertEquals(1, count(result));
    }

    @Test
    public void testWithin() {
        Table pointTable = createPointTable(testDataSize);
        String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
        String expr = "ST_Within(geom_point, ST_GeomFromWkt('" + polygon + "'))";
        Table result = pointTable.filter(expr);
        assertEquals(1, count(result));
    }

    @Test
    public void testCovers() {
        Table pointTable = createPointTable(testDataSize);
        String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
        String expr = "ST_Covers(ST_GeomFromWkt('" + polygon + "'), geom_point)";
        Table result = pointTable.filter(expr);
        assertEquals(1, count(result));
    }

    @Test
    public void testCoveredBy() {
        Table pointTable = createPointTable(testDataSize);
        String polygon = createPolygonWKT(testDataSize).get(0).getField(0).toString();
        String expr = "ST_CoveredBy(geom_point, ST_GeomFromWkt('" + polygon + "'))";
        Table result = pointTable.filter(expr);
        assertEquals(1, count(result));
    }

    @Test
    public void testCrosses() {
        Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('MULTIPOINT((0 0), (2 2))') AS g1, ST_GeomFromWKT('LINESTRING(-1 -1, 1 1)') as g2");
        table = table.select(call(Predicates.ST_Crosses.class.getSimpleName(), $("g1"), $("g2")));
        Boolean actual = (Boolean) first(table).getField(0);
        assertEquals(true, actual);
    }

    @Test
    public void testEquals() {
        Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 2)') AS g1, ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)') as g2");
        table = table.select(call(Predicates.ST_Equals.class.getSimpleName(), $("g1"), $("g2")));
        Boolean actual = (Boolean) first(table).getField(0);
        assertEquals(true, actual);
    }

    @Test
    public void testOrderingEquals() {
        Table lineStringTable = createLineStringTable(testDataSize);
        String lineString = createLineStringWKT(testDataSize).get(0).getField(0).toString();
        String expr = "ST_OrderingEquals(ST_GeomFromWkt('" + lineString + "'), geom_linestring)";
        Table result = lineStringTable.filter(expr);
        assertEquals(1, count(result));
    }

    @Test
    public void testOverlaps() {
        Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 2 2)') AS g1, ST_GeomFromWKT('LINESTRING (1 1, 3 3)') as g2");
        table = table.select(call(Predicates.ST_Overlaps.class.getSimpleName(), $("g1"), $("g2")));
        Boolean actual = (Boolean) first(table).getField(0);
        assertEquals(true, actual);
    }

    @Test
    public void testTouches() {
        Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('LINESTRING (0 0, 1 0)') AS g1, ST_GeomFromWKT('LINESTRING (0 0, 1 1)') as g2");
        table = table.select(call(Predicates.ST_Touches.class.getSimpleName(), $("g1"), $("g2")));
        Boolean actual = (Boolean) first(table).getField(0);
        assertEquals(true, actual);
    }

    @Test
    public void testDWithin() {
        Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (0 0)') as origin, ST_GeomFromWKT('POINT (1 0)') as p1");
        table = table.select(call(Predicates.ST_DWithin.class.getSimpleName(), $("origin"), $("p1"), 1));
        Boolean actual = (Boolean) first(table).getField(0);
        assertEquals(true, actual);
    }

    @Test
    public void testDWithinFailure() {
        Table table = tableEnv.sqlQuery("SELECT ST_GeomFromWKT('POINT (0 0)') as origin, ST_GeomFromWKT('POINT (5 0)') as p1");
        table = table.select(call(Predicates.ST_DWithin.class.getSimpleName(), $("origin"), $("p1"), 2));
        Boolean actual = (Boolean) first(table).getField(0);
        assertEquals(false, actual);
    }

//    @Test
//    public void testDWithinException() {
//        Table table = tableEnv.sqlQuery("SELECT ST_GeomFromEWKT('SRID=4326;POINT (0 0)') as origin, ST_GeomFromWKT('POINT (5 0)') as p1");
//        table = table.select(call(Predicates.ST_DWithin.class.getSimpleName(), $("origin"), $("p1"), 2));
//        Boolean actual = (Boolean) first(table).getField(0);
//        assertEquals(false, actual);
//    }
}
