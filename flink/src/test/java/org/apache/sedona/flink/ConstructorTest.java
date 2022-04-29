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
import org.apache.flink.types.Row;
import org.apache.sedona.flink.expressions.Constructors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.Assert.assertEquals;

public class ConstructorTest extends TestBase{

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize();
    }

    @Test
    public void testPointFromText() {
        List<Row> data = createPointWKT(testDataSize);
        Row result = last(createPointTable(testDataSize));
        assertEquals(result.toString(), data.get(data.size() - 1).toString());
    }

    @Test
    public void testPolygonFromText() {
        List<Row> data = createPolygonWKT(testDataSize);
        Row result = last(createPolygonTable(testDataSize));
        assertEquals(result.toString(), data.get(data.size() - 1).toString());
    }

    @Test
    public void testGeomFromWKT() {
        List<Row> data = createPolygonWKT(testDataSize);
        Table wktTable = createTextTable(data, polygonColNames);
        Table geomTable = wktTable.select(call(Constructors.ST_GeomFromWKT.class.getSimpleName(),
                $(polygonColNames[0])).as(polygonColNames[0]),
                $(polygonColNames[1]));
        Row result = last(geomTable);
        assertEquals(result.toString(), data.get(data.size() - 1).toString());
    }

    @Test
    public void testPolygonFromEnvelope() {
        Double minX = 1.0;
        Double minY = 100.0;
        Double maxX = 2.0;
        Double maxY = 200.0;
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(minX, minY);
        coordinates[1] = new Coordinate(minX, maxY);
        coordinates[2] = new Coordinate(maxX, maxY);
        coordinates[3] = new Coordinate(maxX, minY);
        coordinates[4] = coordinates[0];
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry geom = geometryFactory.createPolygon(coordinates);
        assertEquals(geom.toString(), last(tableEnv.sqlQuery("SELECT ST_PolygonFromEnvelope(1, 100, 2, 200)"))
                .getField(0).toString());
        assertEquals(geom.toString(), last(tableEnv.sqlQuery("SELECT ST_PolygonFromEnvelope(1.0, 100.0, 2.0, 200.0)"))
                .getField(0).toString());

    }

    @Test
    public void testGeomFromGeoJSON() {
        List<Row> data = createPolygonGeoJSON(testDataSize);
        Table geojsonTable = createTextTable(data, polygonColNames);
        Table geomTable = geojsonTable
                .select(call(Constructors.ST_GeomFromGeoJSON.class.getSimpleName(), $(polygonColNames[0]))
                        .as(polygonColNames[0]), $(polygonColNames[1]));
        String result = last(geomTable)
                .getFieldAs(0)
                .toString();

        GeoJSONReader reader = new GeoJSONReader();
        String expectedGeoJSON = data.get(data.size() - 1)
                .getFieldAs(0);
        String expectedGeom = reader.read(expectedGeoJSON).toText();

        assertEquals(result, expectedGeom);
    }

    @Test
    public void testGeomFromGeoHash() {
        Integer precision = 2;
        List<Row> data = new ArrayList<>();
        data.add(Row.of("2131s12fd", "polygon"));

        Table geohashTable = createTextTable(data, polygonColNames);
        Table geomTable = geohashTable
                .select(call(Constructors.ST_GeomFromGeoHash.class.getSimpleName(),
                        $(polygonColNames[0]), precision)
                        .as(polygonColNames[0]), $(polygonColNames[1]));
        String result = first(geomTable)
                .getFieldAs(0)
                        .toString();
        String expectedGeom = "POLYGON ((-180 -39.375, -180 -33.75, -168.75 -33.75, -168.75 -39.375, -180 -39.375))";

        assertEquals(result, expectedGeom);
    }

    @Test
    public void testGeomFromGeoHashNullPrecision() {
        List<Row> data = new ArrayList<>();
        data.add(Row.of("2131s12fd", "polygon"));

        Table geohashTable = createTextTable(data, polygonColNames);
        Table geomTable = geohashTable
                .select(call(Constructors.ST_GeomFromGeoHash.class.getSimpleName(),
                        $(polygonColNames[0]))
                        .as(polygonColNames[0]), $(polygonColNames[1]));
        String result = first(geomTable)
                .getFieldAs(0)
                .toString();
        String expectedGeom = "POLYGON ((-178.4168529510498 -37.69778251647949, -178.4168529510498 -37.697739601135254, -178.41681003570557 -37.697739601135254, -178.41681003570557 -37.69778251647949, -178.4168529510498 -37.69778251647949))";

        assertEquals(result, expectedGeom);
    }
}
