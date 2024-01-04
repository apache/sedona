/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.snowflake.snowsql;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.SQLException;

@RunWith(SnowTestRunner.class)
public class TestConstructors extends TestBase{
    @Test
    public void test_ST_GeomFromWKT() throws SQLException {
        // execute DDL
        registerUDF("ST_GeomFromWKT", String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_GeomFromWKT('Point(0.0 1.0)'))",
                "POINT (0 1)"
        );
    }

    @Test
    public void test_ST_GeomFromGeoHash() {
        registerUDF("ST_GeomFromGeoHash", String.class, Integer.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_GeomFromGeoHash('s00twy01mt', 4))",
                "POLYGON ((0.703125 0.87890625, 0.703125 1.0546875, 1.0546875 1.0546875, 1.0546875 0.87890625, 0.703125 0.87890625))"
        );
    }
    @Test
    public void test_ST_GeomFromGeoJSON() {
        registerUDF("ST_GeomFromGeoJSON", String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[-48.23456,20.12345]}'))",
                "POINT (-48.23456 20.12345)"
        );
    }
    @Test
    public void test_ST_GeomFromGML() {
        registerUDF("ST_GeomFromGML", String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_GeomFromGML('<gml:Point><gml:coordinates>-48.23456,20.12345</gml:coordinates></gml:Point>'))",
                "POINT (-48.23456 20.12345)"
        );
    }
    @Test
    public void test_ST_GeomFromKML() {
        registerUDF("ST_GeomFromKML", String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_GeomFromKML('<LineString><coordinates>-71.1663,42.2614 -71.1667,42.2616</coordinates></LineString>'))",
                "LINESTRING (-71.1663 42.2614, -71.1667 42.2616)"
        );
    }
    @Test
    public void test_ST_GeomFromText() {
        registerUDF("ST_GeomFromText", String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_GeomFromText('POINT (0 1)'))",
                "POINT (0 1)"
        );
        registerUDF("ST_GeomFromText", String.class, int.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_GeomFromText('POINT (0 1)', 4326))",
                "POINT (0 1)"
        );
    }
    @Test
    public void test_ST_GeomFromWKB() {
        registerUDF("ST_GeomFromWKB", byte[].class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_GeomFromWKB(ST_ASWKB(to_geometry('POINT (0.0 1.0)'))))",
                "POINT (0 1)"
        );
    }
    @Test
    public void test_ST_LineFromText() {
        registerUDF("ST_LineFromText", String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_LineFromText('LINESTRING (0 0.0, 1.0 1, 2 2)'))",
                "LINESTRING (0 0, 1 1, 2 2)"
        );
    }
    @Test
    public void test_ST_LineStringFromText() {
        registerUDF("ST_LineStringFromText", String.class, String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_LineStringFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794', ','))",
                "LINESTRING (-74.0428197 40.6867969, -74.0421975 40.6921336, -74.050802 40.6912794)"
        );
    }
    @Test
    public void test_ST_MLineFromText() {
        registerUDF("ST_MLineFromText", String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_MLineFromText('MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))'))",
                "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))"
        );
        registerUDF("ST_MLineFromText", String.class, int.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_MLineFromText('MULTILINESTRING((1 2, 3 4), (4 5, 6 7))',4269))",
                "MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))"
        );
    }
    @Test
    public void test_ST_MPolyFromText() {
        registerUDF("ST_MPolyFromText", String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_MPolyFromText('MULTIPOLYGON (((0 0, 1 1, 1 0, 0 0)), ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))'))",
                "MULTIPOLYGON (((0 0, 1 1, 1 0, 0 0)), ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))"
        );
        registerUDF("ST_MPolyFromText", String.class, int.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_MPolyFromText('MULTIPOLYGON(((1 2, 3 4, 5 6, 1 2)), ((7 8, 9 10, 11 12, 7 8)))',4269))",
                "MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((7 8, 9 10, 11 12, 7 8)))"
        );
    }
    @Test
    public void test_ST_Point() {
        registerUDF("ST_Point", double.class, double.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_Point(1.23, 2.3))",
                "POINT (1.23 2.3)"
        );
    }

    @Test
    public void test_ST_PointZ() {
        registerUDF("ST_PointZ", double.class, double.class, double.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_PointZ(1.23, 2.3, 3.4))",
                "POINT Z(1.23 2.3 3.4)"
        );
        registerUDF("ST_PointZ", double.class, double.class, double.class, int.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_PointZ(1.23, 2.3, 3.4, 4326))",
                "POINT Z(1.23 2.3 3.4)"
        );
    }

    @Test
    public void test_ST_PointFromText() {
        registerUDF("ST_PointFromText", String.class, String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_PointFromText('1.23,2.3', ','))",
                "POINT (1.23 2.3)"
        );
    }
    @Test
    public void test_ST_PolygonFromEnvelope() {
        registerUDF("ST_PolygonFromEnvelope", double.class, double.class, double.class, double.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_PolygonFromEnvelope(1.23, 2.3, 3.4, 4.5))",
                "POLYGON ((1.23 2.3, 1.23 4.5, 3.4 4.5, 3.4 2.3, 1.23 2.3))"
        );
    }
    @Test
    public void test_ST_PolygonFromText() {
        registerUDF("ST_PolygonFromText", String.class, String.class);
        verifySqlSingleRes(
                "select sedona.ST_AsText(sedona.ST_PolygonFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794,-74.0428197,40.6867969', ','))",
                "POLYGON ((-74.0428197 40.6867969, -74.0421975 40.6921336, -74.050802 40.6912794, -74.0428197 40.6867969))"
        );
    }
}
