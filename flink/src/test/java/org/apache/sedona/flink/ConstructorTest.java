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
import org.locationtech.jts.geom.Polygon;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

public class ConstructorTest extends TestBase{

    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize();
    }

    @Test
    public void test2DPoint() {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1.0, 2.0 , "point"));
        String[] colNames = new String[]{"x", "y", "name_point"};

        TypeInformation<?>[] colTypes = {
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO};
        RowTypeInfo typeInfo = new RowTypeInfo(colTypes, colNames);
        DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
        Table pointTable = tableEnv.fromDataStream(ds);

        Table geomTable = pointTable
                .select(call(Constructors.ST_Point.class.getSimpleName(), $(colNames[0]), $(colNames[1]))
                        .as(colNames[2]));

        String result = first(geomTable)
                .getFieldAs(colNames[2])
                .toString();

        String expected = "POINT (1 2)";

        assertEquals(expected, result);
    }

    @Test
    public void testPointFromText() {
        List<Row> data = createPointWKT(testDataSize);
        Row result = last(createPointTable(testDataSize));
        assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
    }

    @Test
    public void testLineFromText() {
        List<Row> data = createLineStringWKT(testDataSize);

        Table lineStringTable = createLineStringTextTable(testDataSize)
                .select(call(Constructors.ST_LineFromText.class.getSimpleName(), $(linestringColNames[0])).as(linestringColNames[0]),
                        $(linestringColNames[1]));
        Row result = last(lineStringTable);

        assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
    }

    @Test
    public void testLineStringFromText() {
        List<Row> data = createLineStringWKT(testDataSize);

        Table lineStringTable = createLineStringTextTable(testDataSize)
                .select(call(Constructors.ST_LineStringFromText.class.getSimpleName(), $(linestringColNames[0])).as(linestringColNames[0]),
                        $(linestringColNames[1]));
        Row result = last(lineStringTable);

        assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
    }

    @Test
    public void testPolygonFromText() {
        List<Row> data = createPolygonWKT(testDataSize);
        Row result = last(createPolygonTable(testDataSize));
        assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
    }

    @Test
    public void testGeomFromWKT() {
        List<Row> data = createPolygonWKT(testDataSize);
        Table wktTable = createTextTable(data, polygonColNames);
        Table geomTable = wktTable.select(call(Constructors.ST_GeomFromWKT.class.getSimpleName(),
                $(polygonColNames[0])).as(polygonColNames[0]),
                $(polygonColNames[1]));
        Row result = last(geomTable);
        assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
    }

    @Test
    public void testGeomFromText() {
        List<Row> data = createPolygonWKT(testDataSize);
        Table wktTable = createTextTable(data, polygonColNames);
        Table geomTable = wktTable.select(call(Constructors.ST_GeomFromText.class.getSimpleName(),
                        $(polygonColNames[0])).as(polygonColNames[0]),
                $(polygonColNames[1]));
        Row result = last(geomTable);
        assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
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
        assertEquals(last(tableEnv.sqlQuery("SELECT ST_PolygonFromEnvelope(1, 100, 2, 200)"))
                .getField(0).toString(), geom.toString());
        assertEquals(last(tableEnv.sqlQuery("SELECT ST_PolygonFromEnvelope(1.0, 100.0, 2.0, 200.0)"))
                .getField(0).toString(), geom.toString());

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

        assertEquals(expectedGeom, result);
    }

    @Test
    public void testGeomFromWKBBytes()
    {
        byte[] wkb = new byte[]{1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75, -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65};
        List<Row> data = new ArrayList<>();
        data.add(Row.of(wkb, "polygon"));
        TypeInformation<?>[] colTypes = {
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO};
        RowTypeInfo typeInfo = new RowTypeInfo(colTypes, Arrays.copyOfRange(polygonColNames, 0, 2));
        DataStream<Row> wkbDS = env.fromCollection(data).returns(typeInfo);
        Table wkbTable = tableEnv.fromDataStream(wkbDS, $(polygonColNames[0]), $(polygonColNames[1]));

        Table geomTable = wkbTable.select(
                call(Constructors.ST_GeomFromWKB.class.getSimpleName(), $(polygonColNames[0])).
                    as(polygonColNames[0]), $(polygonColNames[1]));
        String result = first(geomTable).
            getFieldAs(0).toString();

        String expectedGeom = "LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)";

        assertEquals(expectedGeom, result);

       }

    @Test
    public void testGeomFromGeoHash() {
        Integer precision = 2;
        List<Row> data = new ArrayList<>();
        data.add(Row.of("2131s12fd", "polygon", 0L));

        Table geohashTable = createTextTable(data, polygonColNames);
        Table geomTable = geohashTable
                .select(call(Constructors.ST_GeomFromGeoHash.class.getSimpleName(),
                        $(polygonColNames[0]), precision)
                        .as(polygonColNames[0]), $(polygonColNames[1]));
        String result = first(geomTable)
                .getFieldAs(0)
                        .toString();
        String expectedGeom = "POLYGON ((-180 -39.375, -180 -33.75, -168.75 -33.75, -168.75 -39.375, -180 -39.375))";

        assertEquals(expectedGeom, result);
    }

    @Test
    public void testGeomFromGeoHashNullPrecision() {
        List<Row> data = new ArrayList<>();
        data.add(Row.of("2131s12fd", "polygon", 0L));

        Table geohashTable = createTextTable(data, polygonColNames);
        Table geomTable = geohashTable
                .select(call(Constructors.ST_GeomFromGeoHash.class.getSimpleName(),
                        $(polygonColNames[0]))
                        .as(polygonColNames[0]), $(polygonColNames[1]));
        String result = first(geomTable)
                .getFieldAs(0)
                .toString();
        String expectedGeom = "POLYGON ((-178.4168529510498 -37.69778251647949, -178.4168529510498 -37.697739601135254, -178.41681003570557 -37.697739601135254, -178.41681003570557 -37.69778251647949, -178.4168529510498 -37.69778251647949))";

        assertEquals(expectedGeom, result);
    }

    @Test
    public void testGeomFromGML() {
        List<Row> data = new ArrayList<>();
        String gml =
                "<gml:Polygon>\n" +
                "  <gml:outerBoundaryIs>\n" +
                "    <gml:LinearRing>\n" +
                "      <gml:coordinates>\n" +
                "        0.0,0.0 0.0,1.0 1.0,1.0 1.0,0.0 0.0,0.0\n" +
                "      </gml:coordinates>\n" +
                "    </gml:LinearRing>\n" +
                "  </gml:outerBoundaryIs>\n" +
                "</gml:Polygon>";
        data.add(Row.of(gml, "polygon", 0L));

        Table wktTable = createTextTable(data, polygonColNames);
        Table geomTable = wktTable.select(call(Constructors.ST_GeomFromGML.class.getSimpleName(),
                $(polygonColNames[0])).as(polygonColNames[0]),
                $(polygonColNames[1]));
        assertTrue(first(geomTable).getField(0) instanceof Polygon);
    }

    @Test
    public void testGeomFromKML() {
        List<Row> data = new ArrayList<>();
        String kml =
                "<Polygon>\n" +
                "  <outerBoundaryIs>\n" +
                "    <LinearRing>\n" +
                "      <coordinates>0.0,0.0 0.0,1.0 1.0,1.0 1.0,0.0 0.0,0.0</coordinates>\n" +
                "    </LinearRing>\n" +
                "  </outerBoundaryIs>\n" +
                "</Polygon>";
        data.add(Row.of(kml, "polygon", 0L));

        Table wktTable = createTextTable(data, polygonColNames);
        Table geomTable = wktTable.select(call(Constructors.ST_GeomFromKML.class.getSimpleName(),
                $(polygonColNames[0])).as(polygonColNames[0]),
                $(polygonColNames[1]));
        assertTrue(first(geomTable).getField(0) instanceof Polygon);
    }

    @Test
    public void testMPolygonFromText() {
        List<Row> data = createMultiPolygonText(testDataSize);
        Row result = last(createMultiPolygonTable(testDataSize));
        assertEquals(data.get(data.size() - 1).getField(0).toString(), result.getField(0).toString());
    }

    @Test
    public void testMLineFromText() {
        List<Row> data = new ArrayList<>();
        data.add(Row.of("MULTILINESTRING((1 2, 3 4), (4 5, 6 7))", "multiline", 0L));

        Table geohashTable = createTextTable(data, multilinestringColNames);
        Table geomTable = geohashTable
                .select(call(Constructors.ST_MLineFromText.class.getSimpleName(),
                        $(multilinestringColNames[0]))
                        .as(multilinestringColNames[0]), $(multilinestringColNames[1]));
        String result = first(geomTable)
                .getFieldAs(0)
                .toString();
        String expectedGeom = "MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))";
        assertEquals(expectedGeom, result);
    }
}
