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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.sedona.flink.expressions.Constructors;
import org.locationtech.jts.geom.*;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.*;

public class TestBase {
    protected static StreamExecutionEnvironment env;
    protected static StreamTableEnvironment tableEnv;
    static int testDataSize = 1000;
    static String[] pointColNames = {"geom_point", "name_point", "event_time", "proc_time"};
    static String[] linestringColNames = {"geom_linestring", "name_linestring", "event_time", "proc_time"};
    static String[] multilinestringColNames = {"geom_multilinestring", "name_multilinestring", "event_time", "proc_time"};
    static String[] polygonColNames = {"geom_polygon", "name_polygon", "event_time", "proc_time"};
    static String[] multipolygonColNames = {"geom_multipolygon", "name_multipolygon", "event_time", "proc_time"};
    static String pointTableName = "point_table";
    static String polygonTableName = "polygon_table";
    static Long timestamp_base = new Timestamp(System.currentTimeMillis()).getTime();
    static Long time_interval = 1L; // Generate a record per this interval. Unit is second

    public void setTestDataSize(int testDataSize) {
        this.testDataSize = testDataSize;
    }

    static void initialize() {
        initialize(false);
    }

    static void initialize(boolean enableWebUI) {
        env = enableWebUI? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration()):
                StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        tableEnv = StreamTableEnvironment.create(env, settings);
        SedonaFlinkRegistrator.registerType(env);
        SedonaFlinkRegistrator.registerFunc(tableEnv);
    }

    static List<Row> createPointText(int size){
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create a number of points (1, 1) (2, 2) ...
            data.add(Row.of(i + "," + i, "point" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Point> creatPoint(int size){
        List<Point> data = new ArrayList<>();
        GeometryFactory geomFact = new GeometryFactory();
        for (int i = 0; i < size; i++) {
            // Create a number of points (1, 1) (2, 2) ...
            data.add(geomFact.createPoint(new Coordinate(i, i)));
        }
        return data;
    }

    // Simulate some points in the US
    static List<Row> createPointText_real(int size){
        List<Row> data = new ArrayList<>();
        double x = 32.0;
        double y = -118.0;
        double increment = 10.0/size;
        for (int i = 0; i < size; i++) {
            x += increment;
            y += increment;
            data.add(Row.of(x + "," + y, "point" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createPointWKT(int size){
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create a number of points (1, 1) (2, 2) ...
            data.add(Row.of("POINT (" + i + " " + i +")", "point" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createPolygonText(int size) {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create polygons each of which only has 1 match in points
            // Each polygon is an envelope like (-0.5, -0.5, 0.5, 0.5)
            String minX = String.valueOf(i - 0.5);
            String minY = String.valueOf(i - 0.5);
            String maxX = String.valueOf(i + 0.5);
            String maxY = String.valueOf(i + 0.5);
            List<String> polygon = new ArrayList<>();
            polygon.add(minX);polygon.add(minY);
            polygon.add(minX);polygon.add(maxY);
            polygon.add(maxX);polygon.add(maxY);
            polygon.add(maxX);polygon.add(minY);
            polygon.add(minX);polygon.add(minY);
            data.add(Row.of(String.join(",", polygon), "polygon" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createLineStringText(int size) {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create polygons each of which only has 1 match in points
            // Each polygon is an envelope like (-0.5, -0.5, 0.5, 0.5)
            String minX = String.valueOf(i - 0.5);
            String minY = String.valueOf(i - 0.5);
            String maxX = String.valueOf(i + 0.5);
            String maxY = String.valueOf(i + 0.5);
            List<String> linestring = new ArrayList<>();
            linestring.add(minX);
            linestring.add(minY);
            linestring.add(maxX);
            linestring.add(maxY);

            data.add(Row.of(String.join(",", linestring), "linestring" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createLineStringWKT(int size) {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {

            String minX = String.valueOf(i - 0.5);
            String minY = String.valueOf(i - 0.5);
            String maxX = String.valueOf(i + 0.5);
            String maxY = String.valueOf(i + 0.5);

            List<String> linestring = new ArrayList<>();
            linestring.add(minX + " " + minY);
            linestring.add(maxX + " " + maxY);

            data.add(Row.of("LINESTRING (" + String.join(", ", linestring) + ")", "linestring" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    // createPolyOverlapping
    static List<Row> createPolygonOverlapping(int size) {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create polygons each of which only has 1 match in points
            // The polygons are like (-1, 0, 1, 1)
            // (0, 0, 2, 1)
            // (1, 0, 3, 1)
            String minX = String.valueOf(i - 1);
            String minY = String.valueOf(0);
            String maxX = String.valueOf(i + 1);
            String maxY = String.valueOf(1);
            List<String> polygon = new ArrayList<>();
            polygon.add(minX);polygon.add(minY);
            polygon.add(minX);polygon.add(maxY);
            polygon.add(maxX);polygon.add(maxY);
            polygon.add(maxX);polygon.add(minY);
            polygon.add(minX);polygon.add(minY);
            data.add(Row.of(String.join(",", polygon),"polygon" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createPolygonWKT(int size) {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // Create polygons each of which only has 1 match in points
            // Each polygon is an envelope like (-0.5, -0.5, 0.5, 0.5)
            String minX = String.valueOf(i - 0.5);
            String minY = String.valueOf(i - 0.5);
            String maxX = String.valueOf(i + 0.5);
            String maxY = String.valueOf(i + 0.5);
            List<String> polygon = new ArrayList<>();
            polygon.add(minX + " " + minY);
            polygon.add(minX + " " + maxY);
            polygon.add(maxX + " " + maxY);
            polygon.add(maxX + " " + minY);
            polygon.add(minX + " " + minY);
            data.add(Row.of("POLYGON ((" + String.join(", ", polygon) + "))", "polygon" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static String createPolygonString(int i) {
        // Each polygon is an envelope like (-0.5, -0.5, 0.5, 0.5)
        String minX = String.valueOf(i - 0.5);
        String minY = String.valueOf(i - 0.5);
        String maxX = String.valueOf(i + 0.5);
        String maxY = String.valueOf(i + 0.5);
        List<String> polygon = new ArrayList<>();
        polygon.add(minX + " " + minY);
        polygon.add(minX + " " + maxY);
        polygon.add(maxX + " " + maxY);
        polygon.add(maxX + " " + minY);
        polygon.add(minX + " " + minY);
        return "(("+ String.join(", ", polygon)+"))";
    }

    static List<Row> createMultiPolygonText(int size) {
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            List<String> multiPolygon=new ArrayList<>();
            multiPolygon.add(createPolygonString(i));
            multiPolygon.add(createPolygonString(size-i-1));
            data.add(Row.of("MULTIPOLYGON (" + String.join(", ", multiPolygon) + ")", "multipolygon" + i, timestamp_base + time_interval * 1000 * i));
        }
        return data;
    }

    static List<Row> createPolygonGeoJSON(int size) {
        List<Row> data = new ArrayList<>();
        GeometryFactory geometryFactory = new GeometryFactory();
        GeoJSONWriter writer = new GeoJSONWriter();

        Coordinate[] points = new Coordinate[5];
        for (int i = 0; i < size; i++) {
            double minX = i - 0.5;
            double minY = i - 0.5;
            double maxX = i + 0.5;
            double maxY = i + 0.5;

            points[0] = new Coordinate(minX, minY);
            points[1] = new Coordinate(minX, maxY);
            points[2] = new Coordinate(maxX, maxY);
            points[3] = new Coordinate(maxX, minY);
            points[4] = new Coordinate(minX, minY);

            Geometry polygon = geometryFactory.createPolygon(points);

            String geoJson = writer.write(polygon).toString();
            data.add(Row.of(geoJson, "polygon" + i, timestamp_base + time_interval * 1000 * i));
        }

        return data;
    }

    static Table createTextTable(List<Row> data, String[] colNames){
        TypeInformation<?>[] colTypes = {
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };
        RowTypeInfo typeInfo = new RowTypeInfo(colTypes, Arrays.copyOfRange(colNames, 0, 3));
        DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
        // Generate Time Attribute
        WatermarkStrategy<Row> wmStrategy =
                WatermarkStrategy
                        .<Row>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getFieldAs(2));
        return tableEnv.fromDataStream(ds.assignTimestampsAndWatermarks(wmStrategy), $(colNames[0]), $(colNames[1]), $(colNames[2]).rowtime(), $(colNames[3]).proctime());
    }

    static Table createPointTextTable(int size){
        return createTextTable(createPointText(size), pointColNames);
    }

    static Table createPointTextTable_real(int size){
        return createTextTable(createPointText_real(size), pointColNames);
    }

    static Table createLineStringTextTable(int size) {
        return createTextTable(createLineStringText(size), linestringColNames);
    }

    static Table createPolygonTextTable(int size) {
        return createTextTable(createPolygonText(size), polygonColNames);
    }

    static Table createMultiPolygonTextTable(int size) {
        return createTextTable(createMultiPolygonText(size), multipolygonColNames);
    }

    static Table createPolygonTextOverlappingTable(int size) {
        return createTextTable(createPolygonOverlapping(size), polygonColNames);
    }

    static Table createPointTable(int size){
        return createPointTextTable(size)
                .select(call(Constructors.ST_PointFromText.class.getSimpleName(),
                        $(pointColNames[0])).as(pointColNames[0]),
                        $(pointColNames[1]), $(pointColNames[2]), $(pointColNames[3]));
    }

    static Table createPointTable_real(int size){
        return createPointTextTable_real(size)
                .select(call(Constructors.ST_PointFromText.class.getSimpleName(),
                        $(pointColNames[0])).as(pointColNames[0]),
                        $(pointColNames[1]), $(pointColNames[2]), $(pointColNames[3]));
    }

    static Table createLineStringTable(int size) {
        return createLineStringTextTable(size)
                .select(call(Constructors.ST_LineStringFromText.class.getSimpleName(),
                        $(linestringColNames[0])).as(linestringColNames[0]),
                        $(linestringColNames[1]), $(linestringColNames[2]), $(linestringColNames[3]));
    }

    Table createPolygonTable(int size) {
        return createPolygonTextTable(size)
                .select(call(Constructors.ST_PolygonFromText.class.getSimpleName(),
                        $(polygonColNames[0])).as(polygonColNames[0]),
                        $(polygonColNames[1]), $(polygonColNames[2]), $(polygonColNames[3]));
    }

    Table createMultiPolygonTable(int size) {
        return createMultiPolygonTextTable(size)
                .select(call(Constructors.ST_MPolyFromText.class.getSimpleName(),
                                $(multipolygonColNames[0])).as(multipolygonColNames[0]),
                        $(multipolygonColNames[1]), $(multipolygonColNames[2]), $(multipolygonColNames[3]));
    }
    //createPolygonTextOverlapping

    Table createPolygonOverlappingTable(int size) {
        return createPolygonTextOverlappingTable(size)
                .select(call(Constructors.ST_PolygonFromText.class.getSimpleName(),
                                $(polygonColNames[0])).as(polygonColNames[0]),
                        $(polygonColNames[1]), $(polygonColNames[2]), $(polygonColNames[3]));
    }

    /**
     * Get the iterator of the table
     * @param table
     * @return
     */
    static CloseableIterator<Row> iterate(Table table) {
        return table.execute().collect();
    }

    /**
     * Iterate to the last row of the table
     * @param table
     * @return
     */
    static Row last(Table table) {
        CloseableIterator<Row> it = iterate(table);
        Row lastRow = Row.of(-1L);
        while (it.hasNext()) lastRow = it.next();
        return lastRow;
    }

    /**
     * Get the first row of the table
     * @param table
     * @return
     */
    static Row first(Table table) {
        CloseableIterator<Row> it = iterate(table);
        assert(it.hasNext());
        Row firstRow = it.next();
        return firstRow;
    }

    static List<Row> take(Table table, int n) {
        CloseableIterator<Row> it = iterate(table);
        List<Row> rows = new ArrayList<>();
        while (it.hasNext() && rows.size() < n ) {
            rows.add(it.next());
        }
        return rows;
    }

    static long count(Table table) {
        CloseableIterator<Row> it = iterate(table);
        long count = 0;
        while (it.hasNext()) {
            count++;
            it.next();
        }
        return count;
    }
}