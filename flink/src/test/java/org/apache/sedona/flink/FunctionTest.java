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

import org.apache.flink.table.api.Table;
import org.apache.sedona.flink.expressions.Functions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.Assert.assertEquals;

public class FunctionTest extends TestBase{
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize();
    }

    @Test
    public void testFlipCoordinates() {
        Table pointTable = createPointTable_real(testDataSize);
        Table flippedTable = pointTable.select(call(Functions.ST_FlipCoordinates.class.getSimpleName(), $(pointColNames[0])));
        Geometry result = (Geometry) first(flippedTable).getField(0);
        assertEquals("POINT (-118 32)", result.toString());
    }

    @Test
    public void testTransform() {
        Table pointTable = createPointTable_real(testDataSize);
        Table transformedTable = pointTable.select(call(Functions.ST_Transform.class.getSimpleName(), $(pointColNames[0])
                , "epsg:4326", "epsg:3857"));
        String result = first(transformedTable).getField(0).toString();
        assertEquals("POINT (-13135699.91360628 3763310.6271446524)", result);
    }

    @Test
    public void testDistance() {
        Table pointTable = createPointTable(testDataSize);
        pointTable = pointTable.select(call(Functions.ST_Distance.class.getSimpleName(), $(pointColNames[0])
                , call("ST_GeomFromWKT", "POINT (0 0)")));
        assertEquals(0.0, first(pointTable).getField(0));
    }

    @Test
    public void testGeomToGeoHash() {
        Table pointTable = createPointTable(testDataSize);
        pointTable = pointTable.select(
                call("ST_GeoHash", $(pointColNames[0]), 5)
        );
        assertEquals(first(pointTable).getField(0), Optional.of("s0000"));
    }

    @Test
    public void testPointOnSurface() {
        Table pointTable = createPointTable_real(testDataSize);
        Table surfaceTable = pointTable.select(call(Functions.ST_PointOnSurface.class.getSimpleName(), $(pointColNames[0])));
        Geometry result = (Geometry) first(surfaceTable).getField(0);
        assertEquals("POINT (32 -118)", result.toString());
    }

    @Test
    public void testReverse() {
        Table polygonTable = createPolygonTable(1);
        Table ReversedTable = polygonTable.select(call(Functions.ST_Reverse.class.getSimpleName(), $(polygonColNames[0])));
        Geometry result = (Geometry) first(ReversedTable).getField(0);
        assertEquals("POLYGON ((-0.5 -0.5, 0.5 -0.5, 0.5 0.5, -0.5 0.5, -0.5 -0.5))", result.toString());
    }

    @Test
    public void testAsEWKT() {
        Table polygonTable = createPolygonTable(testDataSize);
        polygonTable = polygonTable.select(call(Functions.ST_AsEWKT.class.getSimpleName(), $(polygonColNames[0])));
        String result = (String) first(polygonTable).getField(0);
        assertEquals("POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))", result);
    }
}
