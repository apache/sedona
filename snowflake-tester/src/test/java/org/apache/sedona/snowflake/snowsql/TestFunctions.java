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

import java.sql.SQLException;
import java.util.regex.Pattern;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(SnowTestRunner.class)
public class TestFunctions extends TestBase {
  @Test
  public void test_GeometryType() {
    registerUDF("GeometryType", byte[].class);
    verifySqlSingleRes("select sedona.GeometryType(sedona.ST_GeomFromText('POINT(1 2)'))", "POINT");
  }

  @Test
  public void test_ST_3DDistance() {
    registerUDF("ST_3DDistance", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_3DDistance(sedona.ST_GeomFromText('POINT (0 0)'), sedona.ST_GeomFromText('POINT (0 1)'))",
        1.0);
  }

  @Test
  public void test_ST_AddPoint() {
    registerUDF("ST_AddPoint", byte[].class, byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_AddPoint(sedona.ST_GeomFromText('LINESTRING (0 0, 1 1)'), sedona.ST_GeomFromText('POINT (0 1)'), 1))",
        "LINESTRING (0 0, 0 1, 1 1)");
  }

  @Test
  public void test_ST_Affine() {
    registerUDF(
        "ST_Affine",
        byte[].class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Affine(sedona.ST_GeomFromText('POINT (1 1)'), 2, 0, 0, 0, 2, 0))",
        "POINT (4 0)");
    registerUDF(
        "ST_Affine",
        byte[].class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Affine(sedona.ST_GeomFromText('POINT (1 1)'), 2, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0))",
        "POINT (2 2)");
  }

  @Test
  public void test_ST_Angle() {
    registerUDF("ST_Angle", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Angle(sedona.ST_GeomFromText('LINESTRING (0 0, 1 1)'), sedona.ST_GeomFromText('LINESTRING (0 0, 1 0)'))",
        0.7853981633974483);
    registerUDF("ST_Angle", byte[].class, byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Angle(sedona.ST_GeomFromText('POINT (1 1)'), sedona.ST_GeomFromText('POINT (2 2)'), sedona.ST_GeomFromText('POINT (3 3)'))",
        3.141592653589793);
    registerUDF("ST_Angle", byte[].class, byte[].class, byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Angle(sedona.ST_GeomFromText('POINT (1 1)'), sedona.ST_GeomFromText('POINT (2 2)'), sedona.ST_GeomFromText('POINT (3 3)'), sedona.ST_GeomFromText('POINT (4 4)'))",
        0.0);
  }

  @Test
  public void test_ST_Area() {
    registerUDF("ST_Area", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Area(sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        1.0);
  }

  @Test
  public void test_ST_AsBinary() {
    registerUDF("ST_AsBinary", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsBinary(sedona.ST_GeomFromText('POINT (0 1)')) = ST_ASWKB(TO_GEOMETRY('POINT (0 1)'))",
        true);
  }

  @Test
  public void test_ST_AsEWKB() throws SQLException {
    registerUDF("ST_AsEWKB", byte[].class);
    registerUDF("ST_SetSRID", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsEWKB(sedona.ST_SetSrid(sedona.ST_GeomFromText('POINT (1 1)'), 3021))",
        new byte[] {
          1, 1, 0, 0, 32, -51, 11, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63
        });
  }

  @Test
  public void test_ST_AsHEXEWKB() throws SQLException {
    registerUDF("ST_AsHEXEWKB", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsHEXEWKB(sedona.ST_GeomFromText('POINT(1 2)'))",
        "0101000000000000000000F03F0000000000000040");

    registerUDF("ST_AsHEXEWKB", byte[].class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsHEXEWKB(sedona.ST_GeomFromText('POINT(1 2)'), 'XDR')",
        "00000000013FF00000000000004000000000000000");
  }

  @Test
  public void test_ST_AsEWKT() {
    registerUDF("ST_AsEWKT", byte[].class);
    registerUDF("ST_SetSRID", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsEWKT(sedona.ST_GeomFromText('POINT (0 1)'))", "POINT (0 1)");
    verifySqlSingleRes(
        "select sedona.ST_AsEWKT(sedona.st_setSRID(sedona.ST_GeomFromText('POINT (0 1)'), 4326))",
        "SRID=4326;POINT (0 1)");
  }

  @Test
  public void test_ST_AsGeoJSON() {
    registerUDF("ST_AsGeoJSON", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsGeoJSON(sedona.ST_GeomFromText('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))'))",
        "{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]}");

    registerUDF("ST_AsGeoJSON", byte[].class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsGeoJSON(sedona.ST_GeomFromText('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))'), 'feature')",
        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]},\"properties\":{}}");

    registerUDF("ST_AsGeoJSON", byte[].class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsGeoJSON(sedona.ST_GeomFromText('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))'), 'featurecollection')",
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]},\"properties\":{}}]}");
  }

  @Test
  public void test_ST_AsGML() {
    registerUDF("ST_AsGML", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsGML(sedona.ST_GeomFromText('POINT (0 1)'))",
        "<gml:Point>\n  <gml:coordinates>\n    0.0,1.0 \n  </gml:coordinates>\n</gml:Point>\n");
  }

  @Test
  public void test_ST_AsKML() {
    registerUDF("ST_AsKML", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsKML(sedona.ST_GeomFromText('POINT (0 1)'))",
        "<Point>\n" + "  <coordinates>0.0,1.0</coordinates>\n" + "</Point>\n");
  }

  @Test
  public void test_ST_AsText() {
    registerUDF("ST_AsText", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_GeomFromText('POINT (0 1)'))", "POINT (0 1)");
  }

  @Test
  public void test_ST_Azimuth() {
    registerUDF("ST_Azimuth", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Azimuth(sedona.ST_GeomFromText('POINT (-71.064544 42.28787)'), sedona.ST_GeomFromText('POINT (-88.331492 32.324142)'))",
        240.0133139011053 * Math.PI / 180);
  }

  @Test
  public void test_ST_Boundary() {
    registerUDF("ST_Boundary", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Boundary(sedona.ST_GeomFromText('POLYGON (( 10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130 ),( 70 40, 100 50, 120 80, 80 110, 50 90, 70 40 ))')))",
        "MULTILINESTRING ((10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130), (70 40, 100 50, 120 80, 80 110, 50 90, 70 40))");
  }

  @Test
  public void test_ST_BoundingDiagonal() {
    registerUDF("ST_BoundingDiagonal", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_BoundingDiagonal(sedona.ST_GeomFromText('POLYGON (( 10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130 ),( 70 40, 100 50, 120 80, 80 110, 50 90, 70 40 ))')))",
        "LINESTRING (10 10, 150 190)");
  }

  @Test
  public void test_ST_Buffer() {
    registerUDF("ST_Buffer", byte[].class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Buffer(sedona.ST_GeomFromText('POINT (0 1)'), 1))",
        "POLYGON ((1 1, 0.9807852804032304 0.8049096779838718, 0.9238795325112867 0.6173165676349102, 0.8314696123025452 0.4444297669803978, 0.7071067811865476 0.2928932188134525, 0.5555702330196023 0.1685303876974548, 0.3826834323650898 0.0761204674887133, 0.1950903220161283 0.0192147195967696, 0.0000000000000001 0, -0.1950903220161282 0.0192147195967696, -0.3826834323650897 0.0761204674887133, -0.555570233019602 0.1685303876974547, -0.7071067811865475 0.2928932188134524, -0.8314696123025453 0.4444297669803978, -0.9238795325112867 0.6173165676349102, -0.9807852804032304 0.8049096779838714, -1 0.9999999999999999, -0.9807852804032304 1.1950903220161284, -0.9238795325112868 1.3826834323650896, -0.8314696123025455 1.555570233019602, -0.7071067811865477 1.7071067811865475, -0.5555702330196022 1.8314696123025453, -0.3826834323650903 1.9238795325112865, -0.1950903220161287 1.9807852804032304, -0.0000000000000002 2, 0.1950903220161283 1.9807852804032304, 0.38268343236509 1.9238795325112865, 0.5555702330196018 1.8314696123025453, 0.7071067811865474 1.7071067811865477, 0.8314696123025452 1.5555702330196022, 0.9238795325112865 1.3826834323650905, 0.9807852804032303 1.1950903220161286, 1 1))");
    registerUDF("ST_Buffer", byte[].class, double.class, boolean.class);
    registerUDF("ST_ReducePrecision", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_ReducePrecision(sedona.ST_Buffer(sedona.ST_GeomFromText('LINESTRING(5 15, -5 15)'), 100, true), 4))",
        "POLYGON ((-5.0002 14.9991, -5.0003 14.9992, -5.0005 14.9993, -5.0006 14.9994, -5.0007 14.9995, -5.0008 14.9997, -5.0009 14.9998, -5.0009 15, -5.0009 15.0002, -5.0008 15.0003, -5.0007 15.0005, -5.0006 15.0006, -5.0005 15.0007, -5.0003 15.0008, -5.0002 15.0009, -5 15.0009, 5 15.0009, 5.0002 15.0009, 5.0003 15.0008, 5.0005 15.0007, 5.0006 15.0006, 5.0007 15.0005, 5.0008 15.0003, 5.0009 15.0002, 5.0009 15, 5.0009 14.9998, 5.0008 14.9997, 5.0007 14.9995, 5.0006 14.9994, 5.0005 14.9993, 5.0003 14.9992, 5.0002 14.9991, 5 14.9991, -5 14.9991, -5.0002 14.9991))");
  }

  @Test
  public void test_ST_BestSRID() {
    registerUDF("ST_BestSRID", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_BestSRID(sedona.ST_GeomFromText('POINT (-180 60)'))", 32660);
  }

  @Test
  public void test_ST_ShiftLongitude() {
    registerUDF("ST_ShiftLongitude", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_ShiftLongitude(sedona.ST_GeomFromText('LINESTRING (179.95 10, -179.95 10)')))",
        "LINESTRING (179.95 10, 180.05 10)");
  }

  @Test
  public void test_ST_BuildArea() {
    registerUDF("ST_BuildArea", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_BuildArea(sedona.ST_GeomFromText('MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(8 8, 8 12, 12 12, 12 8, 8 8),(10 8, 10 12))')))",
        "MULTIPOLYGON (((0 0, 0 20, 20 20, 20 0, 0 0), (2 2, 18 2, 18 18, 2 18, 2 2)), ((8 8, 8 12, 12 12, 12 8, 8 8)))");
  }

  @Test
  public void test_ST_Centroid() {
    registerUDF("ST_Centroid", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Centroid(sedona.ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')))",
        "POINT (5 5)");
  }

  @Test
  public void test_ST_ClosestPoint() {
    registerUDF("ST_ClosestPoint", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_ClosestPoint(sedona.ST_GeomFromText('POINT (1 1)'), sedona.ST_GeomFromText('LINESTRING (1 1, 2 2, 3 3)')))",
        "POINT (1 1)");
  }

  @Test
  public void test_ST_CollectionExtract() {
    registerUDF("ST_CollectionExtract", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_CollectionExtract(sedona.ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))')));",
        "MULTILINESTRING ((1 2, 3 4))");
  }

  @Test
  public void test_ST_ConcaveHull() {
    registerUDF("ST_ConcaveHull", byte[].class, double.class);
    registerUDF("ST_ConcaveHull", byte[].class, double.class, boolean.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_ConcaveHull(sedona.ST_GeomFromText('MULTIPOINT ((10 72), (53 76), (56 66), (63 58), (71 51), (81 48), (91 46), (101 45), (111 46), (121 47), (131 50), (140 55), (145 64), (144 74), (135 80), (125 83), (115 85), (105 87), (95 89), (85 91), (75 93), (65 95), (55 98), (45 102), (37 107), (29 114), (22 122), (19 132), (18 142), (21 151), (27 160), (35 167), (44 172), (54 175), (64 178), (74 180), (84 181), (94 181), (104 181), (114 181), (124 181), (134 179), (144 177), (153 173), (162 168), (171 162), (177 154), (182 145), (184 135), (139 132), (136 142), (128 149), (119 153), (109 155), (99 155), (89 155), (79 153), (69 150), (61 144), (63 134), (72 128), (82 125), (92 123), (102 121), (112 119), (122 118), (132 116), (142 113), (151 110), (161 106), (170 102), (178 96), (185 88), (189 78), (190 68), (189 58), (185 49), (179 41), (171 34), (162 29), (153 25), (143 23), (133 21), (123 19), (113 19), (102 19), (92 19), (82 19), (72 21), (62 22), (52 25), (43 29), (33 34), (25 41), (19 49), (14 58), (21 73), (31 74), (42 74), (173 134), (161 134), (150 133), (97 104), (52 117), (157 156), (94 171), (112 106), (169 73), (58 165), (149 40), (70 33), (147 157), (48 153), (140 96), (47 129), (173 55), (144 86), (159 67), (150 146), (38 136), (111 170), (124 94), (26 59), (60 41), (71 162), (41 64), (88 110), (122 34), (151 97), (157 56), (39 146), (88 33), (159 45), (47 56), (138 40), (129 165), (33 48), (106 31), (169 147), (37 122), (71 109), (163 89), (37 156), (82 170), (180 72), (29 142), (46 41), (59 155), (124 106), (157 80), (175 82), (56 50), (62 116), (113 95), (144 167))'), 0.1))",
        "POLYGON ((18 142, 21 151, 27 160, 35 167, 44 172, 54 175, 64 178, 74 180, 84 181, 94 181, 104 181, 114 181, 124 181, 134 179, 144 177, 153 173, 162 168, 171 162, 177 154, 182 145, 184 135, 173 134, 161 134, 150 133, 139 132, 136 142, 128 149, 119 153, 109 155, 99 155, 89 155, 79 153, 69 150, 61 144, 63 134, 72 128, 82 125, 92 123, 102 121, 112 119, 122 118, 132 116, 142 113, 151 110, 161 106, 170 102, 178 96, 185 88, 189 78, 190 68, 189 58, 185 49, 179 41, 171 34, 162 29, 153 25, 143 23, 133 21, 123 19, 113 19, 102 19, 92 19, 82 19, 72 21, 62 22, 52 25, 43 29, 33 34, 25 41, 19 49, 14 58, 10 72, 21 73, 31 74, 42 74, 53 76, 56 66, 63 58, 71 51, 81 48, 91 46, 101 45, 111 46, 121 47, 131 50, 140 55, 145 64, 144 74, 135 80, 125 83, 115 85, 105 87, 95 89, 85 91, 75 93, 65 95, 55 98, 45 102, 37 107, 29 114, 22 122, 19 132, 18 142))");
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_ConcaveHull(sedona.ST_GeomFromText('MULTIPOINT ((132 64), (114 64), (99 64), (81 64), (63 64), (57 49), (52 36), (46 20), (37 20), (26 20), (32 36), (39 55), (43 69), (50 84), (57 100), (63 118), (68 133), (74 149), (81 164), (88 180), (101 180), (112 180), (119 164), (126 149), (132 131), (139 113), (143 100), (150 84), (157 69), (163 51), (168 36), (174 20), (163 20), (150 20), (143 36), (139 49), (132 64), (99 151), (92 138), (88 124), (81 109), (74 93), (70 82), (83 82), (99 82), (112 82), (126 82), (121 96), (114 109), (110 122), (103 138), (99 151), (34 27), (43 31), (48 44), (46 58), (52 73), (63 73), (61 84), (72 71), (90 69), (101 76), (123 71), (141 62), (166 27), (150 33), (159 36), (146 44), (154 53), (152 62), (146 73), (134 76), (143 82), (141 91), (130 98), (126 104), (132 113), (128 127), (117 122), (112 133), (119 144), (108 147), (119 153), (110 171), (103 164), (92 171), (86 160), (88 142), (79 140), (72 124), (83 131), (79 118), (68 113), (63 102), (68 93), (35 45))'), 0.15, true))",
        "POLYGON ((43 69, 50 84, 57 100, 63 118, 68 133, 74 149, 81 164, 88 180, 101 180, 112 180, 119 164, 126 149, 132 131, 139 113, 143 100, 150 84, 157 69, 163 51, 168 36, 174 20, 163 20, 150 20, 143 36, 139 49, 132 64, 114 64, 99 64, 81 64, 63 64, 57 49, 52 36, 46 20, 37 20, 26 20, 32 36, 35 45, 39 55, 43 69), (88 124, 81 109, 74 93, 83 82, 99 82, 112 82, 121 96, 114 109, 110 122, 103 138, 92 138, 88 124))");
  }

  @Test
  public void test_ST_CoordDim() {
    registerUDF("ST_CoordDim", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_CoordDim(sedona.ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))",
        2);
  }

  @Test
  public void test_ST_ConvexHull() {
    registerUDF("ST_ConvexHull", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_ConvexHull(sedona.ST_GeomFromText('MULTILINESTRING((100 190,10 8),(150 10, 20 30))')))",
        "POLYGON ((10 8, 20 30, 100 190, 150 10, 10 8))");
  }

  @Test
  public void test_ST_CrossesDateLine() {
    registerUDF("ST_CrossesDateLine", byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CrossesDateLine(sedona.ST_GeomFromWKT('POLYGON((175 10, -175 10, -175 -10, 175 -10, 175 10))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CrossesDateLine(sedona.ST_GeomFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        false);
  }

  @Test
  public void test_ST_Difference() {
    registerUDF("ST_Difference", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Difference(sedona.ST_GeomFromText('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'), sedona.ST_GeomFromText('POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))')))",
        "POLYGON ((0 -3, -3 -3, -3 3, 0 3, 0 -3))");
  }

  @Test
  public void test_ST_DelaunayTriangles() {
    registerUDF("ST_DelaunayTriangles", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_DelaunayTriangles(sedona.ST_GeomFromText('POLYGON ((10 10, 15 30, 20 25, 25 35, 30 20, 40 30, 50 10, 45 5, 35 15, 30 5, 25 15, 20 10, 15 20, 10 10))')))",
        "GEOMETRYCOLLECTION (POLYGON ((15 30, 10 10, 15 20, 15 30)), POLYGON ((15 30, 15 20, 20 25, 15 30)), POLYGON ((15 30, 20 25, 25 35, 15 30)), POLYGON ((25 35, 20 25, 30 20, 25 35)), POLYGON ((25 35, 30 20, 40 30, 25 35)), POLYGON ((40 30, 30 20, 35 15, 40 30)), POLYGON ((40 30, 35 15, 50 10, 40 30)), POLYGON ((50 10, 35 15, 45 5, 50 10)), POLYGON ((30 5, 45 5, 35 15, 30 5)), POLYGON ((30 5, 35 15, 25 15, 30 5)), POLYGON ((30 5, 25 15, 20 10, 30 5)), POLYGON ((30 5, 20 10, 10 10, 30 5)), POLYGON ((10 10, 20 10, 15 20, 10 10)), POLYGON ((15 20, 20 10, 25 15, 15 20)), POLYGON ((15 20, 25 15, 20 25, 15 20)), POLYGON ((20 25, 25 15, 30 20, 20 25)), POLYGON ((30 20, 25 15, 35 15, 30 20)))");
  }

  @Test
  public void test_ST_Degrees() {
    registerUDF("ST_Degrees", double.class);
    verifySqlSingleRes("select sedona.ST_Degrees(1)", 57.29577951308232);
  }

  @Test
  public void test_ST_Dimension() {
    registerUDF("ST_Dimension", byte[].class);
    verifySqlSingleRes("select sedona.ST_Dimension(sedona.ST_Point(1, 2))", 0);
  }

  @Test
  public void test_ST_Distance() {
    registerUDF("ST_Distance", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Distance(sedona.ST_GeomFromText('POINT(1 2)'), sedona.ST_GeomFromText('POINT(3 2)'))",
        2.0);
  }

  @Test
  public void test_ST_DumpPoints() {
    registerUDF("ST_DumpPoints", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_DumpPoints(sedona.ST_GeomFromText('MULTILINESTRING((10 40, 40 30), (20 20, 30 10))')))",
        "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))");
  }

  @Test
  public void test_ST_EndPoint() {
    registerUDF("ST_EndPoint", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_EndPoint(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4)')))",
        "POINT (3 4)");
  }

  @Test
  public void test_ST_Envelope() {
    registerUDF("ST_Envelope", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Envelope(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4)')))",
        "POLYGON ((1 2, 1 4, 3 4, 3 2, 1 2))");
  }

  @Test
  public void test_ST_Expand() {
    registerUDF("ST_Expand", byte[].class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Expand(sedona.ST_GeomFromText('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))'), 10))",
        "POLYGON Z((40 40 -9, 40 90 -9, 90 90 13, 90 40 13, 40 40 -9))");

    registerUDF("ST_Expand", byte[].class, double.class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Expand(sedona.ST_GeomFromText('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))'), 5, 6))",
        "POLYGON Z((45 44 1, 45 86 1, 85 86 3, 85 44 3, 45 44 1))");

    registerUDF("ST_Expand", byte[].class, double.class, double.class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Expand(sedona.ST_GeomFromText('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))'), 6, 5, -3))",
        "POLYGON Z((44 45 4, 44 85 4, 86 85 0, 86 45 0, 44 45 4))");
  }

  @Test
  public void test_ST_ExteriorRing() {
    registerUDF("ST_ExteriorRing", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_ExteriorRing(sedona.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')))",
        "LINESTRING (0 0, 0 1, 1 1, 1 0, 0 0)");
  }

  @Test
  public void test_ST_FlipCoordinates() {
    registerUDF("ST_FlipCoordinates", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_FlipCoordinates(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4)')))",
        "LINESTRING (2 1, 4 3)");
  }

  @Test
  public void test_ST_Force_2D() {
    registerUDF("ST_PointZ", double.class, double.class, double.class);
    registerUDF("ST_Force_2D", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Force_2D(sedona.ST_POINTZ(1, 2, 3)))", "POINT (1 2)");
  }

  @Test
  public void test_ST_Force2D() {
    registerUDF("ST_PointZ", double.class, double.class, double.class);
    registerUDF("ST_Force2D", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Force2D(sedona.ST_POINTZ(1, 2, 3)))", "POINT (1 2)");
  }

  @Test
  public void test_ST_GeneratePoints() {
    registerUDF("ST_GeneratePoints", byte[].class, int.class);
    registerUDF("ST_NumGeometries", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_NumGeometries(sedona.ST_GeneratePoints(sedona.ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'), 15))",
        15);

    registerUDF("ST_GeneratePoints", byte[].class, int.class, long.class);
    verifySqlSingleRes(
        "select sedona.ST_NumGeometries(sedona.ST_GeneratePoints(sedona.ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'), 15, 100))",
        15);
  }

  @Test
  public void test_ST_GeoHash() {
    registerUDF("ST_GeoHash", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_GeoHash(sedona.ST_GeomFromText('POINT(21.427834 52.042576573)'), 5)",
        "u3r0p");
  }

  @Test
  public void test_ST_GeometryN() {
    registerUDF("ST_GeometryN", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_GeometryN(sedona.ST_GeomFromText('MULTIPOINT((10 40), (40 30), (20 20), (30 10))'), 1))",
        "POINT (40 30)");
  }

  @Test
  public void test_ST_GeometryType() {
    registerUDF("ST_GeometryType", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_GeometryType(sedona.ST_GeomFromText('POINT(1 2)'))", "ST_Point");
  }

  @Test
  public void test_ST_HasZ() {
    registerUDF("ST_HasZ", byte[].class);
    verifySqlSingleRes("SELECT sedona.ST_HasZ(sedona.ST_GeomFromText('POINT Z(1 2 3)'))", true);
  }

  @Test
  public void test_ST_HausdorffDistance() {
    registerUDF("ST_HausdorffDistance", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_HausdorffDistance(sedona.ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'), sedona.ST_GeomFromText('LINESTRING(0 0, 1 1, 3 3)'))",
        1.4142135623730951);
  }

  @Test
  public void test_ST_InteriorRingN() {
    registerUDF("ST_InteriorRingN", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_InteriorRingN(sedona.ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))'), 0))",
        "LINESTRING (1 1, 2 1, 2 2, 1 2, 1 1)");
  }

  @Test
  public void test_ST_Intersection() {
    registerUDF("ST_Intersection", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Intersection(sedona.ST_GeomFromText('LINESTRING(0 0, 2 2)'), sedona.ST_GeomFromText('LINESTRING(0 2, 2 0)')))",
        "POINT (1 1)");
  }

  @Test
  public void test_ST_IsClosed() {
    registerUDF("ST_IsClosed", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_IsClosed(sedona.ST_GeomFromText('LINESTRING(0 0, 2 2)'))", false);
  }

  @Test
  public void test_ST_IsCollection() {
    registerUDF("ST_IsCollection", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_IsCollection(sedona.ST_GeomFromText('LINESTRING(0 0, 2 2)'))", false);
    verifySqlSingleRes(
        "select sedona.ST_IsCollection(sedona.ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))'))",
        true);
  }

  @Test
  public void test_ST_IsEmpty() {
    registerUDF("ST_IsEmpty", byte[].class);
    verifySqlSingleRes("select sedona.ST_IsEmpty(sedona.ST_GeomFromText('POINT(1 2)'))", false);
  }

  @Test
  public void test_ST_IsRing() {
    registerUDF("ST_IsRing", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_IsRing(sedona.ST_GeomFromText('LINESTRING(0 0, 2 2)'))", false);
    verifySqlSingleRes(
        "select sedona.ST_IsRing(sedona.ST_GeomFromText('LINESTRING(0 0, 2 2, 1 2, 0 0)'))", true);
  }

  @Test
  public void test_ST_IsSimple() {
    registerUDF("ST_IsSimple", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_IsSimple(sedona.ST_GeomFromText('LINESTRING(0 0, 2 2)'))", true);
    verifySqlSingleRes(
        "select sedona.ST_IsSimple(sedona.ST_GeomFromText('POLYGON((1 1,3 1,3 3,2 0,1 1))'))",
        false);
  }

  @Test
  public void test_ST_IsValid() {
    registerUDF("ST_IsValid", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_IsValid(sedona.ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))'))",
        false);
    registerUDF("ST_IsValid", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_IsValid(sedona.ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))'), 1)",
        false);
  }

  @Test
  public void test_ST_IsValidReason() {
    registerUDF("ST_IsValidReason", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_IsValidReason(sedona.ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))'))",
        "Hole lies outside shell at or near point (15.0, 15.0)");
    registerUDF("ST_IsValidReason", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_IsValidReason(sedona.ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))'), 1)",
        "Hole lies outside shell at or near point (15.0, 15.0)");
  }

  @Test
  public void test_ST_Length() {
    registerUDF("ST_Length", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Length(sedona.ST_GeomFromText('LINESTRING(0 0, 2 2)'))",
        2.8284271247461903);
  }

  @Test
  public void test_ST_Length2D() {
    registerUDF("ST_Length2D", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Length2D(sedona.ST_GeomFromText('LINESTRING(0 0, 2 2)'))",
        2.8284271247461903);
  }

  @Test
  public void test_ST_LineFromMultiPoint() {
    registerUDF("ST_LineFromMultiPoint", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_LineFromMultiPoint(sedona.ST_GeomFromText('MULTIPOINT((10 40), (40 30), (20 20), (30 10))')))",
        "LINESTRING (10 40, 40 30, 20 20, 30 10)");
  }

  @Test
  public void test_ST_LineInterpolatePoint() {
    registerUDF("ST_LineInterpolatePoint", byte[].class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_LineInterpolatePoint(sedona.ST_GeomFromText('LINESTRING(25 50, 100 125, 150 190)'), 0.2))",
        "POINT (51.5974135047432 76.5974135047432)");
  }

  @Test
  public void test_ST_LineLocatePoint() {
    registerUDF("ST_LineLocatePoint", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_LineLocatePoint(sedona.ST_GeomFromText('LINESTRING(0 0, 10 10)'), sedona.ST_GeomFromText('POINT(2 2)'))",
        0.2);
  }

  @Test
  public void test_ST_LineMerge() {
    registerUDF("ST_LineMerge", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_LineMerge(sedona.ST_GeomFromText('MULTILINESTRING((0 0, 1 1), (1 1, 2 2))')))",
        "LINESTRING (0 0, 1 1, 2 2)");
  }

  @Test
  public void test_ST_LongestLine() {
    registerUDF("ST_LongestLine", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_LongestLine(sedona.ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))'), sedona.ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))')))",
        "LINESTRING (180 180, 20 50)");
  }

  @Test
  public void test_ST_MakeLine() {
    registerUDF("ST_MakeLine", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_MakeLine(sedona.ST_Point(1, 2), sedona.ST_Point(3, 4)))",
        "LINESTRING (1 2, 3 4)");
    registerUDF("ST_MakeLine", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_MakeLine(sedona.ST_GeomFromText('GeometryCollection (POINT (1 2), POINT (3 4))')))",
        "LINESTRING (1 2, 3 4)");
  }

  @Test
  public void test_ST_MaxDistance() {
    registerUDF("ST_MaxDistance", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_MaxDistance(sedona.ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))'), sedona.ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))'))",
        206.15528128088303);
  }

  @Test
  public void test_ST_LineSubstring() {
    registerUDF("ST_LineSubstring", byte[].class, double.class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_LineSubstring(sedona.ST_GeomFromText('LINESTRING (20 180, 50 20, 90 80, 120 40, 180 150)'), 0.333, 0.666))",
        "LINESTRING (45.17311810399485 45.74337011202746, 50 20, 90 80, 112.97593050157862 49.36542599789519)");
  }

  @Test
  public void test_ST_MakePoint() {
    registerUDF("ST_MakePoint", double.class, double.class);
    verifySqlSingleRes("select sedona.ST_AsText(sedona.ST_MakePoint(1, 2))", "POINT (1 2)");
    registerUDF("ST_MakePoint", double.class, double.class, double.class);
    verifySqlSingleRes("select sedona.ST_AsText(sedona.ST_MakePoint(1, 2, 3))", "POINT Z(1 2 3)");
    // TODO: fix this when https://github.com/locationtech/jts/pull/734 gets merged.
    // Sedona Snowflake uses WKB to serialize geometries, and JTS's WKBReader and Writer ignores M
    // values.
    registerUDF("ST_MakePoint", double.class, double.class, double.class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_MakePoint(1, 2, 3, 4))", "POINT Z(1 2 3)");
  }

  @Test
  public void test_ST_MakePolygon() {
    registerUDF("ST_MakePolygon", byte[].class);
    registerUDF("ST_MakePolygon", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_MakePolygon(sedona.ST_GeomFromText('LINESTRING(75 29, 77 29, 77 29, 75 29)')))",
        "POLYGON ((75 29, 77 29, 77 29, 75 29))");
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_MakePolygon(sedona.ST_GeomFromText('LINESTRING(75 29, 77 29, 77 29, 75 29)'), sedona.ST_GeomFromText('MULTILINESTRING ((2 3, 1 4, 2 4, 2 3), (2 4, 3 5, 3 4, 2 4))') ))  ",
        "POLYGON ((75 29, 77 29, 77 29, 75 29), (2 3, 1 4, 2 4, 2 3), (2 4, 3 5, 3 4, 2 4))");
  }

  @Test
  public void test_ST_MakeValid() {
    registerUDF("ST_MakeValid", byte[].class);
    registerUDF("ST_MakeValid", byte[].class, boolean.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_MakeValid(sedona.ST_GeomFromText('POLYGON((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))')))",
        "MULTIPOLYGON (((1 5, 3 3, 1 1, 1 5)), ((5 3, 7 5, 7 1, 5 3)))");
  }

  @Test
  public void test_ST_MinimumClearance() {
    registerUDF("ST_MinimumClearance", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_MinimumClearance(sedona.ST_GeomFromText('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))'))",
        0.5);
  }

  @Test
  public void test_ST_MinimumClearanceLine() {
    registerUDF("ST_MinimumClearanceLine", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_MinimumClearanceLine(sedona.ST_GeomFromText('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))')))",
        "LINESTRING (64.5 16, 65 16)");
  }

  @Test
  public void test_ST_MinimumBoundingCircle() {
    registerUDF("ST_MinimumBoundingCircle", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_MinimumBoundingCircle(sedona.ST_GeomFromText('GeometryCollection (LINESTRING(55 75,125 150), POINT (20 80))') , 8))",
        "POLYGON ((135.5971473206198 115, 134.38475332749803 102.69035721092119, 130.79416296937 90.85376709089948, 124.96336062007234 79.94510316021109, 117.11642074393687 70.38357925606314, 107.55489683978892 62.536639379927664, 96.64623290910052 56.70583703062998, 84.80964278907881 53.11524667250196, 72.5 51.90285267938019, 60.1903572109212 53.11524667250196, 48.35376709089948 56.70583703062998, 37.4451031602111 62.53663937992766, 27.883579256063136 70.38357925606313, 20.036639379927657 79.94510316021109, 14.20583703062998 90.85376709089948, 10.61524667250196 102.69035721092118, 9.402852679380189 114.99999999999999, 10.61524667250196 127.30964278907881, 14.205837030629972 139.14623290910052, 20.03663937992765 150.0548968397889, 27.883579256063122 159.61642074393686, 37.44510316021108 167.46336062007234, 48.35376709089945 173.29416296937, 60.19035721092117 176.88475332749803, 72.49999999999999 178.0971473206198, 84.80964278907881 176.88475332749803, 96.64623290910053 173.29416296937, 107.5548968397889 167.46336062007236, 117.11642074393686 159.61642074393689, 124.96336062007234 150.0548968397889, 130.79416296937 139.14623290910055, 134.38475332749803 127.30964278907884, 135.5971473206198 115))");
  }

  @Test
  public void test_ST_Multi() {
    registerUDF("ST_Multi", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Multi(sedona.ST_GeomFromText('POINT(1 2)')))",
        "MULTIPOINT ((1 2))");
  }

  @Test
  public void test_ST_NDims() {
    registerUDF("ST_NDims", byte[].class);
    verifySqlSingleRes("select sedona.ST_NDims(sedona.ST_GeomFromText('POINT(1 1)'))", 2);
    verifySqlSingleRes("select sedona.ST_NDims(sedona.ST_GeomFromText('POINT(1 1 1)'))", 3);
  }

  @Test
  public void test_ST_Normalize() {
    registerUDF("ST_Normalize", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Normalize(sedona.ST_GeomFromText('POLYGON((0 1, 1 1, 1 0, 0 0, 0 1))')))",
        "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
  }

  @Test
  public void test_ST_NPoints() {
    registerUDF("ST_NPoints", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_NPoints(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'))", 3);
  }

  @Test
  public void test_ST_NumGeometries() {
    registerUDF("ST_NumGeometries", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_NumGeometries(sedona.ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2), POINT(3 4), LINESTRING(1 1, 1 2))'))",
        3);
  }

  @Test
  public void test_ST_NumInteriorRings() {
    registerUDF("ST_NumInteriorRings", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_NumInteriorRings(sedona.ST_GeomFromText('POLYGON((0 0 0,0 5 0,5 0 0,0 0 5),(1 1 0,3 1 0,1 3 0,1 1 0))'))",
        1);
    verifySqlSingleRes(
        "select sedona.ST_NumInteriorRings(sedona.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        0);
  }

  @Test
  public void test_ST_NumInteriorRing() {
    registerUDF("ST_NumInteriorRing", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_NumInteriorRing(sedona.ST_GeomFromText('POLYGON((0 0 0,0 5 0,5 0 0,0 0 5),(1 1 0,3 1 0,1 3 0,1 1 0))'))",
        1);
    verifySqlSingleRes(
        "select sedona.ST_NumInteriorRing(sedona.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        0);
  }

  @Test
  public void test_ST_PointN() {
    registerUDF("ST_PointN", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_PointN(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'), 2))",
        "POINT (3 4)");
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_PointN(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'), -1))",
        "POINT (5 6)");
  }

  @Test
  public void test_ST_PointOnSurface() {
    registerUDF("ST_PointOnSurface", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_PointOnSurface(sedona.ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')))",
        "POINT (2.5 2.5)");
  }

  @Test
  public void test_ST_Points() {
    registerUDF("ST_Points", byte[].class);
    registerUDF("ST_AsEWKT", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsEWKT(sedona.ST_Points(sedona.ST_GeomFromText('LINESTRING(0 0, 0 1, 0 2, 0 3, 0 4)')))",
        "MULTIPOINT ((0 0), (0 1), (0 2), (0 3), (0 4))");
  }

  @Test
  public void test_ST_Polygon() {
    registerUDF("ST_Polygon", byte[].class, int.class);
    registerUDF("ST_AsEWKT", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsEWKT(sedona.ST_Polygon(sedona.ST_GeomFromText('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'), 4326))",
        "SRID=4326;POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
  }

  @Test
  public void test_ST_Polygonize() {
    registerUDF("ST_Polygonize", byte[].class);
    registerUDF("ST_Area", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_Area(sedona.ST_Polygonize(sedona.ST_GeomFromText('GEOMETRYCOLLECTION (LINESTRING (2 0, 2 1, 2 2), LINESTRING (2 2, 2 3, 2 4), LINESTRING (0 2, 1 2, 2 2), LINESTRING (2 2, 3 2, 4 2), LINESTRING (0 2, 1 3, 2 4), LINESTRING (2 4, 3 3, 4 2))')))",
        4.0);
  }

  @Test
  public void test_ST_PrecisionReduce() {
    registerUDF("ST_PrecisionReduce", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_PrecisionReduce(sedona.ST_GeomFromText('POINT(1.123456789 2.123456789)'), 3))",
        "POINT (1.123 2.123)");
  }

  @Test
  public void test_ST_RemovePoint() {
    registerUDF("ST_RemovePoint", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_RemovePoint(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)')))",
        "LINESTRING (1 2, 3 4)");
    registerUDF("ST_RemovePoint", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_RemovePoint(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'), 1))",
        "LINESTRING (1 2, 5 6)");
  }

  @Test
  public void test_ST_Reverse() {
    registerUDF("ST_Reverse", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Reverse(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)')))",
        "LINESTRING (5 6, 3 4, 1 2)");
  }

  @Test
  public void test_ST_S2CellIDs() {
    registerUDF("ST_S2CellIDs", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_S2CellIDs(sedona.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), 9)",
        "[\n  1153031455769624576,\n  1152961087025446912,\n  1152925902653358080,\n  1152934698746380288,\n  1152943494839402496,\n  1152952290932424704,\n  1152969883118469120,\n  1152978679211491328,\n  1152987475304513536,\n  1152996271397535744,\n  1153005067490557952,\n  1153049047955668992,\n  1153057844048691200,\n  1153040251862646784,\n  1153084232327757824,\n  1153093028420780032,\n  1153066640141713408,\n  1153075436234735616,\n  1153101824513802240,\n  1153137008885891072,\n  1153189785444024320,\n  1153198581537046528,\n  1153172193257979904,\n  1153180989351002112,\n  1153163397164957696,\n  1153128212792868864,\n  1153013863583580160,\n  1153022659676602368,\n  1153242562002157568,\n  1153216173723090944,\n  1153277746374246400,\n  1153207377630068736,\n  1153224969816113152,\n  1153233765909135360,\n  1153268950281224192,\n  1153321726839357440,\n  1153365707304468480,\n  1153374503397490688,\n  1153400891676557312,\n  1153409687769579520,\n  1153383299490512896,\n  1153392095583535104,\n  1153436076048646144,\n  1153444872141668352,\n  1153418483862601728,\n  1153427279955623936,\n  1153453668234690560,\n  1153462464327712768,\n  1153330522932379648,\n  1921361385166471168,\n  1921475734375759872,\n  1921484530468782080,\n  1921493326561804288,\n  1921519714840870912,\n  1921528510933893120,\n  1921537307026915328,\n  384305702186778624,\n  1152389340979003392,\n  1152398137072025600,\n  1152406933165047808,\n  1152873126095224832,\n  1152881922188247040,\n  1152890718281269248,\n  1152917106560335872\n]");
  }

  @Test
  public void test_ST_SetPoint() {
    registerUDF("ST_SetPoint", byte[].class, int.class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_SetPoint(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'), 1, sedona.ST_GeomFromText('POINT(10 10)')))",
        "LINESTRING (1 2, 10 10, 5 6)");
  }

  @Test
  public void test_ST_SetSRID() {
    registerUDF("ST_AsEWKT", byte[].class);
    registerUDF("ST_SetSRID", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsEWKT(sedona.ST_SetSRID(sedona.ST_GeomFromText('POINT(1 2)'), 4326))",
        "SRID=4326;POINT (1 2)");
  }

  @Test
  public void test_ST_SimplifyPreserveTopology() {
    registerUDF("ST_SimplifyPreserveTopology", byte[].class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_SimplifyPreserveTopology(sedona.ST_GeomFromText('POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33,46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22,35 26, 24 39, 8 25))'), 10))",
        "POLYGON ((8 25, 28 22, 15 11, 33 3, 56 30, 47 44, 35 36, 43 19, 24 39, 8 25))");
  }

  @Test
  public void test_ST_SimplifyVW() {
    registerUDF("ST_SimplifyVW", byte[].class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_SimplifyVW(sedona.ST_GeomFromText('POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33,46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22,35 26, 24 39, 8 25))'), 10))",
        "POLYGON ((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33, 47 44, 35 36, 45 33, 43 19, 29 21, 35 26, 24 39, 8 25))");
  }

  @Test
  public void test_ST_SimplifyPolygonHull() {
    registerUDF("ST_SimplifyPolygonHull", byte[].class, double.class, boolean.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_SimplifyPolygonHull(sedona.ST_GeomFromText('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))'), 0.3, false))",
        "POLYGON ((30 10, 40 40, 10 20, 30 10))");
    registerUDF("ST_SimplifyPolygonHull", byte[].class, double.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_SimplifyPolygonHull(sedona.ST_GeomFromText('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))'), 0.3))",
        "POLYGON ((30 10, 15 15, 10 20, 20 40, 45 45, 30 10))");
  }

  @Test
  public void test_ST_Split() {
    registerUDF("ST_Split", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Split(sedona.ST_GeomFromText('LINESTRING (0 0, 1.5 1.5, 2 2)'), sedona.ST_GeomFromText('MULTIPOINT (0.5 0.5, 1 1)')))",
        "MULTILINESTRING ((0 0, 0.5 0.5), (0.5 0.5, 1 1), (1 1, 1.5 1.5, 2 2))");
  }

  @Test
  public void test_ST_SRID() {
    registerUDF("ST_SRID", byte[].class);
    verifySqlSingleRes("select sedona.ST_SRID(sedona.ST_GeomFromText('POINT(1 2)'))", 0);
  }

  @Test
  public void test_ST_StartPoint() {
    registerUDF("ST_StartPoint", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_StartPoint(sedona.ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)')))",
        "POINT (1 2)");
  }

  @Test
  public void test_ST_Snap() {
    registerUDF("ST_Snap", byte[].class, byte[].class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Snap(sedona.ST_GeomFromWKT('POLYGON((2.6 12.5, 2.6 20.0, 12.6 20.0, 12.6 12.5, 2.6 12.5 ))'), sedona.ST_GeomFromWKT('LINESTRING (0.5 10.7, 5.4 8.4, 10.1 10.0)'), 2.525))",
        "POLYGON ((2.6 12.5, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 2.6 12.5))");
  }

  @Test
  public void test_ST_SubDivide() {
    registerUDF("ST_SubDivide", byte[].class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_SubDivide(sedona.ST_GeomFromText('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)'), 5))",
        "MULTILINESTRING ((0 0, 2.5 0), (2.5 0, 5 0))");
  }

  @Test
  public void test_ST_SymDifference() {
    registerUDF("ST_SymDifference", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_SymDifference(sedona.ST_GeomFromText('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))'), sedona.ST_GeomFromText('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))')))",
        "MULTIPOLYGON (((0 -1, -1 -1, -1 1, 1 1, 1 0, 0 0, 0 -1)), ((0 -1, 1 -1, 1 0, 2 0, 2 -2, 0 -2, 0 -1)))");
  }

  @Test
  public void test_ST_Transform() {
    registerUDF("ST_Transform", byte[].class, String.class, String.class, boolean.class);
    verifySqlSingleRes(
        "select SEDONA.ST_AsText(SEDONA.ST_Transform(SEDONA.ST_geomFromWKT('POLYGON ((110.54671 55.818002, 110.54671 55.143743, 110.940494 55.143743, 110.940494 55.818002, 110.54671 55.818002))'),'EPSG:4326', 'EPSG:32649', false))",
        Pattern.compile(
            "POLYGON \\(\\(471596.69167460\\d* 6185916.95119\\d*, 471107.562364\\d* 6110880.97422\\d*, 496207.10915\\d* 6110788.80471\\d*, 496271.3193704\\d* 6185825.6056\\d*, 471596.6916746\\d* 6185916.95119\\d*\\)\\)"));
  }

  @Test
  public void test_ST_Union() {
    registerUDF("ST_Union", byte[].class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_Union(sedona.ST_GeomFromText('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'), sedona.ST_GeomFromText('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))')))",
        "POLYGON ((2 3, 3 3, 3 -3, -3 -3, -3 3, -2 3, -2 4, 2 4, 2 3))");
  }

  @Test
  public void test_ST_UnaryUnion() {
    registerUDF("ST_UnaryUnion", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_UnaryUnion(sedona.ST_GeomFromText('MULTIPOLYGON(((0 10,0 30,20 30,20 10,0 10)),((10 0,10 20,30 20,30 0,10 0)))')))",
        "POLYGON ((10 0, 10 10, 0 10, 0 30, 20 30, 20 20, 30 20, 30 0, 10 0))");
  }

  @Test
  public void test_ST_VoronoiPolygons() {
    registerUDF("ST_VoronoiPolygons", byte[].class);
    registerUDF("ST_VoronoiPolygons", byte[].class, double.class);
    registerUDF("ST_VoronoiPolygons", byte[].class, double.class, byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_VoronoiPolygons(sedona.ST_GeomFromText('MULTIPOINT ((0 0), (1 1))')))",
        "GEOMETRYCOLLECTION (POLYGON ((-1 -1, -1 2, 2 -1, -1 -1)), POLYGON ((-1 2, 2 2, 2 -1, -1 2)))");
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_VoronoiPolygons(sedona.ST_GeomFromText('MULTIPOINT ((0 0), (1 1))'), 1))",
        "GEOMETRYCOLLECTION (POLYGON ((-1 -1, -1 2, 2 -1, -1 -1)), POLYGON ((-1 2, 2 2, 2 -1, -1 2)))");
    verifySqlSingleRes(
        "select sedona.ST_AsText(sedona.ST_VoronoiPolygons(sedona.ST_GeomFromText('MULTIPOINT ((0 0), (1 1))'), 1, sedona.ST_GeomFromText('POLYGON ((-1 -1, -1 2, 2 -1, -1 -1))')))",
        "GEOMETRYCOLLECTION (POLYGON ((-1 -1, -1 2, 2 -1, -1 -1)), POLYGON ((-1 2, 2 2, 2 -1, -1 2)))");
  }

  @Test
  public void test_ST_X() {
    registerUDF("ST_X", byte[].class);
    verifySqlSingleRes("select sedona.ST_X(sedona.ST_GeomFromText('POINT(1 2)'))", 1.0);
    verifySqlSingleRes("select sedona.ST_X(sedona.ST_GeomFromText('LINESTRING(1 2, 2 2)'))", null);
  }

  @Test
  public void test_ST_XMax() {
    registerUDF("ST_XMax", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_XMax(sedona.ST_GeomFromText('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))",
        2.0);
  }

  @Test
  public void test_ST_XMin() {
    registerUDF("ST_XMin", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_XMin(sedona.ST_GeomFromText('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))",
        -1.0);
  }

  @Test
  public void test_ST_Y() {
    registerUDF("ST_Y", byte[].class);
    verifySqlSingleRes("select sedona.ST_Y(sedona.ST_GeomFromText('POINT(1 2)'))", 2.0);
    verifySqlSingleRes(
        "select sedona.ST_Y(sedona.ST_GeomFromText('LINESTRING(1 -1, 2 2, 2 3)'))", null);
  }

  @Test
  public void test_ST_YMax() {
    registerUDF("ST_YMax", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_YMax(sedona.ST_GeomFromText('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))",
        12.0);
  }

  @Test
  public void test_ST_YMin() {
    registerUDF("ST_YMin", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_YMin(sedona.ST_GeomFromText('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))",
        -11.0);
  }

  @Test
  public void test_ST_Z() {
    registerUDF("ST_Z", byte[].class);
    verifySqlSingleRes("select sedona.ST_Z(sedona.ST_GeomFromText('POINT Z(1 2 3)'))", 3.0);
    verifySqlSingleRes(
        "select sedona.ST_Z(sedona.ST_GeomFromText('LINESTRING Z(1 -1 1, 2 2 2, 2 3 3)'))", null);
  }

  @Test
  public void test_ST_ZMax() {
    registerUDF("ST_ZMax", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_ZMax(sedona.ST_GeomFromText('POLYGON Z((-1 -11 1, 0 10 2, 1 11 3, 2 12 4, -1 -11 5))'))",
        5.0);
  }

  @Test
  public void test_ST_ZMin() {
    registerUDF("ST_ZMin", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_ZMin(sedona.ST_GeomFromText('POLYGON Z((-1 -11 1, 0 10 2, 1 11 3, 2 12 4, -1 -11 5))'))",
        1.0);
  }

  @Test
  public void test_ST_AreaSpheroid() {
    registerUDF("ST_AreaSpheroid", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_AreaSpheroid(sedona.ST_GeomFromText('Polygon ((34 35, 28 30, 25 34, 34 35))'))",
        201824850811.76245);
  }

  @Test
  public void test_ST_DistanceSphere() {
    registerUDF("ST_DistanceSphere", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_DistanceSphere(sedona.ST_GeomFromWKT('POINT (-0.56 51.3168)'), sedona.ST_GeomFromWKT('POINT (-3.1883 55.9533)'))",
        543796.9506134904);
    registerUDF("ST_DistanceSphere", byte[].class, byte[].class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_DistanceSphere(sedona.ST_GeomFromWKT('POINT (-0.56 51.3168)'), sedona.ST_GeomFromWKT('POINT (-3.1883 55.9533)'), 6378137.0)",
        544405.4459192449);
  }

  @Test
  public void test_ST_DistanceSpheroid() {
    registerUDF("ST_DistanceSpheroid", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_DistanceSpheroid(sedona.ST_GeomFromWKT('POINT (-0.56 51.3168)'), sedona.ST_GeomFromWKT('POINT (-3.1883 55.9533)'))",
        544430.9411996207);
  }

  @Test
  public void test_ST_FrechetDistance() {
    registerUDF("ST_FrechetDistance", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_FrechetDistance(sedona.ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'), sedona.ST_GeomFromWKT('LINESTRING (0 0, 1 1, 3 3)'))",
        1.4142135623730951);
  }

  @Test
  public void test_ST_Force3D() {
    registerUDF("ST_Force3D", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Force3D(sedona.ST_GeomFromText('LINESTRING(0 1, 1 2, 2 1)')))",
        "LINESTRING Z(0 1 0, 1 2 0, 2 1 0)");
    registerUDF("ST_Force3D", byte[].class, double.class);
    registerUDF("ST_Force3D", byte[].class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Force3D(sedona.ST_GeomFromText('LINESTRING(0 1, 1 2, 2 1)'), 1))",
        "LINESTRING Z(0 1 1, 1 2 1, 2 1 1)");
  }

  @Test
  public void test_ST_Force3DZ() {
    registerUDF("ST_Force3DZ", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Force3DZ(sedona.ST_GeomFromText('LINESTRING(0 1, 1 2, 2 1)')))",
        "LINESTRING Z(0 1 0, 1 2 0, 2 1 0)");
    registerUDF("ST_Force3DZ", byte[].class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Force3DZ(sedona.ST_GeomFromText('LINESTRING(0 1, 1 2, 2 1)'), 1))",
        "LINESTRING Z(0 1 1, 1 2 1, 2 1 1)");
  }

  @Test
  public void test_ST_ForceCollection() {
    registerUDF("ST_ForceCollection", byte[].class);
    registerUDF("ST_NumGeometries", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_NumGeometries(sedona.ST_ForceCollection(sedona.ST_GeomFromWKT('MULTIPOINT (30 10, 40 40, 20 20, 10 30, 10 10, 20 50)')))",
        6);
  }

  @Test
  public void test_ST_ForcePolygonCW() {
    registerUDF("ST_ForcePolygonCW", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_ForcePolygonCW(sedona.ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')))",
        "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))");
  }

  @Test
  public void test_ST_IsPolygonCW() {
    registerUDF("ST_IsPolygonCW", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_IsPolygonCW(sedona.ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))'))",
        true);
  }

  @Test
  public void test_ST_ForceRHR() {
    registerUDF("ST_ForceRHR", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_ForceRHR(sedona.ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')))",
        "POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))");
  }

  @Test
  public void test_ST_LengthSpheroid() {
    registerUDF("ST_LengthSpheroid", byte[].class);
    verifySqlSingleRes(
        "select sedona.ST_LengthSpheroid(sedona.ST_GeomFromWKT('Polygon ((0 0, 90 0, 0 0))'))",
        20037508.342789244);
  }

  @Test
  public void test_ST_IsPolygonCCW() {
    registerUDF("ST_IsPolygonCCW", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_IsPolygonCCW(sedona.ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))'))",
        true);
  }

  @Test
  public void test_ST_ForcePolygonCCW() {
    registerUDF("ST_ForcePolygonCCW", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_ForcePolygonCCW(sedona.ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))')))",
        "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))");
  }

  @Test
  public void test_ST_GeometricMedian() {
    registerUDF("ST_GeometricMedian", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_asText(sedona.ST_GeometricMedian(sedona.ST_GeomFromWKT('MULTIPOINT((0 0), (1 1), (2 2), (200 200))')))",
        "POINT (1.9761550281255005 1.9761550281255005)");
    registerUDF("ST_GeometricMedian", byte[].class, float.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_asText(sedona.ST_GeometricMedian(sedona.ST_GeomFromWKT('MULTIPOINT ((0 -1), (0 0), (0 0), (0 1))'), 1e-6))",
        "POINT (0 0)");
  }

  @Test
  public void test_ST_NRings() {
    registerUDF("ST_NRings", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_NRings(sedona.ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))", 1);
  }

  @Test
  public void test_ST_NumPoints() {
    registerUDF("ST_NumPoints", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_NumPoints(sedona.ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'))", 3);
  }

  @Test
  public void test_ST_TriangulatePolygon() {
    registerUDF("ST_TriangulatePolygon", byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_TriangulatePolygon(sedona.ST_GeomFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))')))",
        "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 10, 5 5, 0 0)), POLYGON ((5 8, 5 5, 0 10, 5 8)), POLYGON ((10 0, 0 0, 5 5, 10 0)), POLYGON ((10 10, 5 8, 0 10, 10 10)), POLYGON ((10 0, 5 5, 8 5, 10 0)), POLYGON ((5 8, 10 10, 8 8, 5 8)), POLYGON ((10 10, 10 0, 8 5, 10 10)), POLYGON ((8 5, 8 8, 10 10, 8 5)))");
  }

  @Test
  public void test_ST_Translate() {
    registerUDF("ST_Translate", byte[].class, double.class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Translate(sedona.ST_GeomFromText('POINT(1 3 2)'), 1, 2))",
        "POINT Z(2 5 2)");
    registerUDF("ST_Translate", byte[].class, double.class, double.class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Translate(sedona.ST_GeomFromText('GEOMETRYCOLLECTION(MULTIPOLYGON (((1 0 0, 1 1 0, 2 1 0, 2 0 0, 1 0 0)), ((1 2 0, 3 4 0, 3 5 0, 1 2 0))), POINT(1 1 1), LINESTRING EMPTY))'), 2, 2, 3))",
        "GEOMETRYCOLLECTION Z(MULTIPOLYGON Z(((3 2 3, 3 3 3, 4 3 3, 4 2 3, 3 2 3)), ((3 4 3, 5 6 3, 5 7 3, 3 4 3))), POINT Z(3 3 4), LINESTRING ZEMPTY)");
  }

  @Test
  public void test_ST_RotateX() {
    registerUDF("ST_RotateX", byte[].class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_RotateX(sedona.ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)'), 10))",
        "LINESTRING (0 0, 1 0, 1 -0.8390715290764524, 0 0)");
  }

  @Test
  public void test_ST_Rotate() {
    registerUDF("ST_Rotate", byte[].class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Rotate(sedona.ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)'), 10))",
        "LINESTRING (0 0, -0.8390715290764524 -0.5440211108893698, -0.2950504181870827 -1.383092639965822, 0 0)");
    registerUDF("ST_Rotate", byte[].class, double.class, byte[].class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Rotate(sedona.ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)'), 10, sedona.ST_GeomFromWKT('POINT (0 0)')))",
        "LINESTRING (0 0, -0.8390715290764524 -0.5440211108893698, -0.2950504181870827 -1.383092639965822, 0 0)");
    registerUDF("ST_Rotate", byte[].class, double.class, double.class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Rotate(sedona.ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)'), 10, 0, 0))",
        "LINESTRING (0 0, -0.8390715290764524 -0.5440211108893698, -0.2950504181870827 -1.383092639965822, 0 0)");
  }
}
