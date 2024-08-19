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
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(SnowTestRunner.class)
public class TestFunctionsV2 extends TestBase {
  @Test
  public void test_GeometryType() {
    registerUDFV2("GeometryType", String.class);
    verifySqlSingleRes("select sedona.GeometryType(ST_GeometryFromWKT('POINT(1 2)'))", "POINT");
  }

  @Test
  public void test_ST_3DDistance() {
    registerUDFV2("ST_3DDistance", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_3DDistance(ST_GeometryFromWKT('POINT (0 0)'), ST_GeometryFromWKT('POINT (0 1)'))",
        1.0);
  }

  @Test
  public void test_ST_AddPoint() {
    registerUDFV2("ST_AddPoint", String.class, String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_AddPoint(ST_GeometryFromWKT('LINESTRING (0 0, 1 1)'), ST_GeometryFromWKT('POINT (0 1)'), 1))",
        "LINESTRING(0 0,0 1,1 1)");
  }

  @Test
  public void test_ST_Affine() {
    registerUDFV2(
        "ST_Affine",
        String.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class,
        double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Affine(ST_GeometryFromWKT('POINT (1 1)'), 2, 0, 0, 0, 2, 0))",
        "POINT(4 0)");
    registerUDFV2(
        "ST_Affine",
        String.class,
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
        "select ST_AsText(sedona.ST_Affine(ST_GeometryFromWKT('POINT (1 1)'), 2, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0))",
        "POINT(2 2)");
  }

  @Test
  public void test_ST_Angle() {
    registerUDFV2("ST_Angle", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_Angle(ST_GeometryFromWKT('LINESTRING (0 0, 1 1)'), ST_GeometryFromWKT('LINESTRING (0 0, 1 0)'))",
        0.7853981633974483);
    registerUDFV2("ST_Angle", String.class, String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_Angle(ST_GeometryFromWKT('POINT (1 1)'), ST_GeometryFromWKT('POINT (2 2)'),ST_GeometryFromWKT('POINT (3 3)'))",
        3.141592653589793);
    registerUDFV2("ST_Angle", String.class, String.class, String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_Angle(ST_GeometryFromWKT('POINT (1 1)'), ST_GeometryFromWKT('POINT (2 2)'), ST_GeometryFromWKT('POINT (3 3)'), ST_GeometryFromWKT('POINT (4 4)'))",
        0.0);
  }

  @Test
  public void test_ST_Area() {
    registerUDFV2("ST_Area", String.class);
    verifySqlSingleRes(
        "select sedona.ST_Area(ST_GeometryFromWKT('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))", 1.0);
  }

  @Test
  public void test_ST_AsBinary() {
    registerUDFV2("ST_AsBinary", String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsBinary(ST_GeometryFromWKT('POINT (0 1)')) = ST_ASWKB(TO_GEOMETRY('POINT (0 1)'))",
        true);
  }

  @Test
  public void test_ST_AsEWKB() throws SQLException {
    registerUDFV2("ST_AsEWKB", String.class);
    registerUDFV2("ST_SetSRID", String.class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_AsEWKB(sedona.ST_SetSrid(ST_GeometryFromWKT('POINT (1 1)'), 3021))",
        new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63});
  }

  @Test
  public void test_ST_AsHEXEWKB() throws SQLException {
    registerUDFV2("ST_AsHEXEWKB", String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsHEXEWKB(ST_GeometryFromWKT('POINT(1 2)'))",
        "0101000000000000000000F03F0000000000000040");

    registerUDFV2("ST_AsHEXEWKB", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsHEXEWKB(ST_GeometryFromWKT('POINT(1 2)'), 'XDR')",
        "00000000013FF00000000000004000000000000000");
  }

  @Test
  public void test_ST_AsEWKT() {
    registerUDFV2("ST_AsEWKT", String.class);
    registerUDFV2("ST_SetSRID", String.class, int.class);
    verifySqlSingleRes("select sedona.ST_AsEWKT(ST_GeometryFromWKT('POINT (0 1)'))", "POINT (0 1)");
    verifySqlSingleRes(
        "select sedona.ST_AsEWKT(sedona.st_setSRID(ST_GeometryFromWKT('POINT (0 1)'), 4326))",
        "POINT (0 1)");
  }

  @Test
  public void test_ST_AsGeoJSON() {
    registerUDFV2("ST_AsGeoJSON", String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsGeoJSON(ST_GeometryFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))'))",
        "{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]}");

    registerUDFV2("ST_AsGeoJSON", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsGeoJSON(ST_GeometryFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))'), 'feature')",
        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]},\"properties\":{}}");

    registerUDFV2("ST_AsGeoJSON", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsGeoJSON(ST_GeometryFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))'), 'featurecollection')",
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[8.0,1.0],[8.0,8.0],[1.0,8.0],[1.0,1.0]]]},\"properties\":{}}]}");
  }

  @Test
  public void test_ST_AsGML() {
    registerUDFV2("ST_AsGML", String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsGML(ST_GeometryFromWKT('POINT (0 1)'))",
        "<gml:Point>\n  <gml:coordinates>\n    0.0,1.0 \n  </gml:coordinates>\n</gml:Point>\n");
  }

  @Test
  public void test_ST_AsKML() {
    registerUDFV2("ST_AsKML", String.class);
    verifySqlSingleRes(
        "select sedona.ST_AsKML(ST_GeometryFromWKT('POINT (0 1)'))",
        "<Point>\n" + "  <coordinates>0.0,1.0</coordinates>\n" + "</Point>\n");
  }

  @Test
  public void test_ST_AsText() {
    registerUDFV2("ST_AsText", String.class);
    verifySqlSingleRes("select sedona.ST_AsText(ST_GeometryFromWKT('POINT (0 1)'))", "POINT (0 1)");
  }

  @Test
  public void test_ST_Azimuth() {
    registerUDFV2("ST_Azimuth", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_Azimuth(ST_GeometryFromWKT('POINT (-71.064544 42.28787)'), ST_GeometryFromWKT('POINT (-88.331492 32.324142)'))",
        240.0133139011053 * Math.PI / 180);
  }

  @Test
  public void test_ST_BestSRID() {
    registerUDFV2("ST_BestSRID", String.class);
    verifySqlSingleRes("select sedona.ST_BestSRID(ST_GeometryFromWKT('POINT (-180 60)'))", 32660);
  }

  @Test
  public void test_ST_ShiftLongitude() {
    registerUDFV2("ST_ShiftLongitude", String.class);
    verifySqlSingleRes(
        "select sedona.ST_ShiftLongitude(ST_GeometryFromWKT('POINT (-179 60)'))",
        "{\"type\":\"Point\",\"coordinates\":[181.0,60.0]}");
  }

  @Test
  public void test_ST_Boundary() {
    registerUDFV2("ST_Boundary", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Boundary(ST_GeometryFromWKT('POLYGON (( 10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130 ),( 70 40, 100 50, 120 80, 80 110, 50 90, 70 40 ))')))",
        "MULTILINESTRING((10 130,50 190,110 190,140 150,150 80,100 10,20 40,10 130),(70 40,100 50,120 80,80 110,50 90,70 40))");
  }

  @Test
  public void test_ST_BoundingDiagonal() {
    registerUDFV2("ST_BoundingDiagonal", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_BoundingDiagonal(ST_GeometryFromWKT('POLYGON (( 10 130, 50 190, 110 190, 140 150, 150 80, 100 10, 20 40, 10 130 ),( 70 40, 100 50, 120 80, 80 110, 50 90, 70 40 ))')))",
        "LINESTRING(10 10,150 190)");
  }

  @Test
  public void test_ST_Buffer() {
    registerUDFV2("ST_Buffer", String.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Buffer(ST_GeometryFromWKT('POINT (0 1)'), 1))",
        "POLYGON((1 1,0.9807852804 0.804909678,0.9238795325 0.6173165676,0.8314696123 0.444429767,0.7071067812 0.2928932188,0.555570233 0.1685303877,0.3826834324 0.07612046749,0.195090322 0.0192147196,6.123233996e-17 0,-0.195090322 0.0192147196,-0.3826834324 0.07612046749,-0.555570233 0.1685303877,-0.7071067812 0.2928932188,-0.8314696123 0.444429767,-0.9238795325 0.6173165676,-0.9807852804 0.804909678,-1 1,-0.9807852804 1.195090322,-0.9238795325 1.382683432,-0.8314696123 1.555570233,-0.7071067812 1.707106781,-0.555570233 1.831469612,-0.3826834324 1.923879533,-0.195090322 1.98078528,-1.836970199e-16 2,0.195090322 1.98078528,0.3826834324 1.923879533,0.555570233 1.831469612,0.7071067812 1.707106781,0.8314696123 1.555570233,0.9238795325 1.382683432,0.9807852804 1.195090322,1 1))");
    registerUDFV2("ST_Buffer", String.class, double.class, boolean.class);
    verifySqlSingleRes(
        "select ST_AsText(ST_ReducePrecision(sedona.ST_Buffer(ST_GeometryFromWKT('LINESTRING(0.05 15, -0.05 15)'), 10000, true), 8))",
        "POLYGON((-0.06809794 14.91143929,-0.0855181 14.91657277,-0.10157458 14.92491089,-0.11565095 14.93613348,-0.12720661 14.94980963,-0.13579747 14.96541421,-0.14109307 14.98234798,-0.14288927 14.99996056,-0.14111623 15.01757534,-0.13584121 15.03451549,-0.12726608 15.05012991,-0.11571978 15.0638183,-0.10164567 15.07505424,-0.08558461 15.0834055,-0.06815417 15.08855069,-0.05002481 15.09029174,0.0500174 15.09029949,0.06814888 15.08856105,0.08558242 15.08341775,0.10164727 15.07506734,0.11572543 15.06383098,0.12727556 15.05014087,0.13585382 15.03452355,0.1411309 15.01757961,0.14290466 14.99996055,0.14110774 14.98234371,0.13581008 14.96540615,0.12721607 14.94979867,0.11565658 14.93612079,0.10157617 14.92489779,0.08551591 14.91656051,0.06809265 14.91142893,0.04997533 14.90969989,-0.04998274 14.90970765,-0.06809794 14.91143929))");
  }

  @Test
  public void test_ST_BuildArea() {
    registerUDFV2("ST_BuildArea", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_BuildArea(ST_GeometryFromWKT('MULTILINESTRING((0 0, 20 0, 20 20, 0 20, 0 0),(2 2, 18 2, 18 18, 2 18, 2 2),(8 8, 8 12, 12 12, 12 8, 8 8),(10 8, 10 12))')))",
        "MULTIPOLYGON(((0 0,0 20,20 20,20 0,0 0),(2 2,18 2,18 18,2 18,2 2)),((8 8,8 12,12 12,12 8,8 8)))");
  }

  @Test
  public void test_ST_Centroid() {
    registerUDFV2("ST_Centroid", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Centroid(ST_GeometryFromWKT('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')))",
        "POINT(5 5)");
  }

  @Test
  public void test_ST_ClosestPoint() {
    registerUDFV2("ST_ClosestPoint", String.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_ClosestPoint(ST_GeometryFromWKT('POINT (1 1)'), ST_GeometryFromWKT('LINESTRING (1 1, 2 2, 3 3)')))",
        "POINT(1 1)");
  }

  @Test
  public void test_ST_CollectionExtract() {
    registerUDFV2("ST_CollectionExtract", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_CollectionExtract(ST_GeometryFromWKT('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))')));",
        "MULTILINESTRING((1 2,3 4))");
  }

  @Test
  public void test_ST_ConcaveHull() {
    registerUDFV2("ST_ConcaveHull", String.class, double.class);
    registerUDFV2("ST_ConcaveHull", String.class, double.class, boolean.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_ConcaveHull(ST_GeometryFromWKT('MULTIPOINT ((10 72), (53 76), (56 66), (63 58), (71 51), (81 48), (91 46), (101 45), (111 46), (121 47), (131 50), (140 55), (145 64), (144 74), (135 80), (125 83), (115 85), (105 87), (95 89), (85 91), (75 93), (65 95), (55 98), (45 102), (37 107), (29 114), (22 122), (19 132), (18 142), (21 151), (27 160), (35 167), (44 172), (54 175), (64 178), (74 180), (84 181), (94 181), (104 181), (114 181), (124 181), (134 179), (144 177), (153 173), (162 168), (171 162), (177 154), (182 145), (184 135), (139 132), (136 142), (128 149), (119 153), (109 155), (99 155), (89 155), (79 153), (69 150), (61 144), (63 134), (72 128), (82 125), (92 123), (102 121), (112 119), (122 118), (132 116), (142 113), (151 110), (161 106), (170 102), (178 96), (185 88), (189 78), (190 68), (189 58), (185 49), (179 41), (171 34), (162 29), (153 25), (143 23), (133 21), (123 19), (113 19), (102 19), (92 19), (82 19), (72 21), (62 22), (52 25), (43 29), (33 34), (25 41), (19 49), (14 58), (21 73), (31 74), (42 74), (173 134), (161 134), (150 133), (97 104), (52 117), (157 156), (94 171), (112 106), (169 73), (58 165), (149 40), (70 33), (147 157), (48 153), (140 96), (47 129), (173 55), (144 86), (159 67), (150 146), (38 136), (111 170), (124 94), (26 59), (60 41), (71 162), (41 64), (88 110), (122 34), (151 97), (157 56), (39 146), (88 33), (159 45), (47 56), (138 40), (129 165), (33 48), (106 31), (169 147), (37 122), (71 109), (163 89), (37 156), (82 170), (180 72), (29 142), (46 41), (59 155), (124 106), (157 80), (175 82), (56 50), (62 116), (113 95), (144 167))'), 0.1))",
        "POLYGON((18 142,21 151,27 160,35 167,44 172,54 175,64 178,74 180,84 181,94 181,104 181,114 181,124 181,134 179,144 177,153 173,162 168,171 162,177 154,182 145,184 135,173 134,161 134,150 133,139 132,136 142,128 149,119 153,109 155,99 155,89 155,79 153,69 150,61 144,63 134,72 128,82 125,92 123,102 121,112 119,122 118,132 116,142 113,151 110,161 106,170 102,178 96,185 88,189 78,190 68,189 58,185 49,179 41,171 34,162 29,153 25,143 23,133 21,123 19,113 19,102 19,92 19,82 19,72 21,62 22,52 25,43 29,33 34,25 41,19 49,14 58,10 72,21 73,31 74,42 74,53 76,56 66,63 58,71 51,81 48,91 46,101 45,111 46,121 47,131 50,140 55,145 64,144 74,135 80,125 83,115 85,105 87,95 89,85 91,75 93,65 95,55 98,45 102,37 107,29 114,22 122,19 132,18 142))");
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_ConcaveHull(ST_GeometryFromWKT('MULTIPOINT ((132 64), (114 64), (99 64), (81 64), (63 64), (57 49), (52 36), (46 20), (37 20), (26 20), (32 36), (39 55), (43 69), (50 84), (57 100), (63 118), (68 133), (74 149), (81 164), (88 180), (101 180), (112 180), (119 164), (126 149), (132 131), (139 113), (143 100), (150 84), (157 69), (163 51), (168 36), (174 20), (163 20), (150 20), (143 36), (139 49), (132 64), (99 151), (92 138), (88 124), (81 109), (74 93), (70 82), (83 82), (99 82), (112 82), (126 82), (121 96), (114 109), (110 122), (103 138), (99 151), (34 27), (43 31), (48 44), (46 58), (52 73), (63 73), (61 84), (72 71), (90 69), (101 76), (123 71), (141 62), (166 27), (150 33), (159 36), (146 44), (154 53), (152 62), (146 73), (134 76), (143 82), (141 91), (130 98), (126 104), (132 113), (128 127), (117 122), (112 133), (119 144), (108 147), (119 153), (110 171), (103 164), (92 171), (86 160), (88 142), (79 140), (72 124), (83 131), (79 118), (68 113), (63 102), (68 93), (35 45))'), 0.15, true))",
        "POLYGON((43 69,50 84,57 100,63 118,68 133,74 149,81 164,88 180,101 180,112 180,119 164,126 149,132 131,139 113,143 100,150 84,157 69,163 51,168 36,174 20,163 20,150 20,143 36,139 49,132 64,114 64,99 64,81 64,63 64,57 49,52 36,46 20,37 20,26 20,32 36,35 45,39 55,43 69),(88 124,81 109,74 93,83 82,99 82,112 82,121 96,114 109,110 122,103 138,92 138,88 124))");
  }

  @Test
  public void test_ST_CoordDim() {
    registerUDFV2("ST_CoordDim", String.class);
    verifySqlSingleRes(
        "select sedona.ST_CoordDim(ST_GeometryFromWKT('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))",
        2);
  }

  @Test
  public void test_ST_ConvexHull() {
    registerUDFV2("ST_ConvexHull", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_ConvexHull(ST_GeometryFromWKT('MULTILINESTRING((100 190,10 8),(150 10, 20 30))')))",
        "POLYGON((10 8,20 30,100 190,150 10,10 8))");
  }

  @Test
  public void test_ST_CrossesDateLine() {
    registerUDFV2("ST_CrossesDateLine", String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CrossesDateLine( ST_GeometryFromWKT('POLYGON((175 10, -175 10, -175 -10, 175 -10, 175 10))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CrossesDateLine( ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        false);
  }

  @Test
  public void test_ST_Difference() {
    registerUDFV2("ST_Difference", String.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Difference(ST_GeometryFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'), ST_GeometryFromWKT('POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))')))",
        "POLYGON((0 -3,-3 -3,-3 3,0 3,0 -3))");
  }

  @Test
  public void test_ST_DelaunayTriangles() {
    registerUDFV2("ST_DelaunayTriangles", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_DelaunayTriangles(ST_GeomFromText('POLYGON ((10 10, 15 30, 20 25, 25 35, 30 20, 40 30, 50 10, 45 5, 35 15, 30 5, 25 15, 20 10, 15 20, 10 10))')))",
        "GEOMETRYCOLLECTION(POLYGON((15 30,10 10,15 20,15 30)),POLYGON((15 30,15 20,20 25,15 30)),POLYGON((15 30,20 25,25 35,15 30)),POLYGON((25 35,20 25,30 20,25 35)),POLYGON((25 35,30 20,40 30,25 35)),POLYGON((40 30,30 20,35 15,40 30)),POLYGON((40 30,35 15,50 10,40 30)),POLYGON((50 10,35 15,45 5,50 10)),POLYGON((30 5,45 5,35 15,30 5)),POLYGON((30 5,35 15,25 15,30 5)),POLYGON((30 5,25 15,20 10,30 5)),POLYGON((30 5,20 10,10 10,30 5)),POLYGON((10 10,20 10,15 20,10 10)),POLYGON((15 20,20 10,25 15,15 20)),POLYGON((15 20,25 15,20 25,15 20)),POLYGON((20 25,25 15,30 20,20 25)),POLYGON((30 20,25 15,35 15,30 20)))");
  }

  @Test
  public void test_ST_Dimension() {
    registerUDFV2("ST_Dimension", String.class);
    verifySqlSingleRes("select sedona.ST_Dimension(ST_GeometryFromWKT('POINT(1 2)'))", 0);
  }

  @Test
  public void test_ST_Distance() {
    registerUDFV2("ST_Distance", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_Distance(ST_GeometryFromWKT('POINT(1 2)'), ST_GeometryFromWKT('POINT(3 2)'))",
        2.0);
  }

  @Test
  public void test_ST_DumpPoints() {
    registerUDFV2("ST_DumpPoints", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_DumpPoints(ST_GeometryFromWKT('MULTILINESTRING((10 40, 40 30), (20 20, 30 10))')))",
        "MULTIPOINT((10 40),(40 30),(20 20),(30 10))");
  }

  @Test
  public void test_ST_EndPoint() {
    registerUDFV2("ST_EndPoint", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_EndPoint(ST_GeometryFromWKT('LINESTRING(1 2, 3 4)')))",
        "POINT(3 4)");
  }

  @Test
  public void test_ST_Envelope() {
    registerUDFV2("ST_Envelope", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Envelope(ST_GeometryFromWKT('LINESTRING(1 2, 3 4)')))",
        "POLYGON((1 2,1 4,3 4,3 2,1 2))");
  }

  @Test
  public void test_ST_Expand() {
    registerUDFV2("ST_Expand", String.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Expand(ST_GeometryFromWKT('POLYGON Z((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))'), 10))",
        "POLYGONZ((40 40 -9,40 90 -9,90 90 13,90 40 13,40 40 -9))");

    registerUDFV2("ST_Expand", String.class, double.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Expand(ST_GeometryFromWKT('POLYGON Z((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))'), 5, 6))",
        "POLYGONZ((45 44 1,45 86 1,85 86 3,85 44 3,45 44 1))");

    registerUDFV2("ST_Expand", String.class, double.class, double.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Expand(ST_GeometryFromWKT('POLYGON Z((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))'), 6, 5, -3))",
        "POLYGONZ((44 45 4,44 85 4,86 85 0,86 45 0,44 45 4))");
  }

  @Test
  public void test_ST_ExteriorRing() {
    registerUDFV2("ST_ExteriorRing", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_ExteriorRing(ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')))",
        "LINESTRING(0 0,0 1,1 1,1 0,0 0)");
  }

  @Test
  public void test_ST_FlipCoordinates() {
    registerUDFV2("ST_FlipCoordinates", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_FlipCoordinates(ST_GeometryFromWKT('LINESTRING(1 2, 3 4)')))",
        "LINESTRING(2 1,4 3)");
  }

  @Test
  public void test_ST_Force_2D() {
    registerUDFV2("ST_Force_2D", String.class);
    verifySqlSingleRes("select ST_AsText(sedona.ST_Force_2D(ST_GEOMPOINT(1, 2)))", "POINT(1 2)");
  }

  @Test
  public void test_ST_Force2D() {
    registerUDFV2("ST_Force2D", String.class);
    verifySqlSingleRes("select ST_AsText(sedona.ST_Force2D(ST_GEOMPOINT(1, 2)))", "POINT(1 2)");
  }

  @Test
  public void test_ST_GeneratePoints() {
    registerUDFV2("ST_GeneratePoints", String.class, int.class);
    registerUDFV2("ST_NumGeometries", String.class);
    verifySqlSingleRes(
        "select sedona.ST_NumGeometries(sedona.ST_GeneratePoints(ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'), 15))",
        15);

    registerUDFV2("ST_GeneratePoints", String.class, int.class, long.class);
    verifySqlSingleRes(
        "select sedona.ST_NumGeometries(sedona.ST_GeneratePoints(ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))'), 15, 100))",
        15);
  }

  @Test
  public void test_ST_GeoHash() {
    registerUDFV2("ST_GeoHash", String.class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_GeoHash(ST_GeometryFromWKT('POINT(21.427834 52.042576573)'), 5)",
        "u3r0p");
  }

  @Test
  public void test_ST_GeometryN() {
    registerUDFV2("ST_GeometryN", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_GeometryN(ST_GeometryFromWKT('MULTIPOINT((10 40), (40 30), (20 20), (30 10))'), 1))",
        "POINT(40 30)");
  }

  @Test
  public void test_ST_GeometryType() {
    registerUDFV2("ST_GeometryType", String.class);
    verifySqlSingleRes(
        "select sedona.ST_GeometryType(ST_GeometryFromWKT('POINT(1 2)'))", "ST_Point");
  }

  @Test
  public void test_ST_HasZ() {
    registerUDFV2("ST_HasZ", String.class);
    verifySqlSingleRes("SELECT sedona.ST_HasZ(ST_GeomFromText('POINT Z(1 2 3)'))", true);
  }

  @Test
  public void test_ST_HausdorffDistance() {
    registerUDFV2("ST_HausdorffDistance", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_HausdorffDistance(ST_GeometryFromWKT('LINESTRING(0 0, 1 1, 2 2)'), ST_GeometryFromWKT('LINESTRING(0 0, 1 1, 3 3)'))",
        1.4142135623730951);
  }

  @Test
  public void test_ST_InteriorRingN() {
    registerUDFV2("ST_InteriorRingN", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_InteriorRingN(ST_GeometryFromWKT('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))'), 0))",
        "LINESTRING(1 1,2 1,2 2,1 2,1 1)");
  }

  @Test
  public void test_ST_Intersection() {
    registerUDFV2("ST_Intersection", String.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Intersection(ST_GeometryFromWKT('LINESTRING(0 0, 2 2)'), ST_GeometryFromWKT('LINESTRING(0 2, 2 0)')))",
        "POINT(1 1)");
  }

  @Test
  public void test_ST_IsClosed() {
    registerUDFV2("ST_IsClosed", String.class);
    verifySqlSingleRes(
        "select sedona.ST_IsClosed(ST_GeometryFromWKT('LINESTRING(0 0, 2 2)'))", false);
  }

  @Test
  public void test_ST_IsCollection() {
    registerUDFV2("ST_IsCollection", String.class);
    verifySqlSingleRes(
        "select sedona.ST_IsCollection(ST_GeometryFromWKT('LINESTRING(0 0, 2 2)'))", false);
    verifySqlSingleRes(
        "select sedona.ST_IsCollection(ST_GeometryFromWKT('GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(1 2, 3 4))'))",
        true);
  }

  @Test
  public void test_ST_IsEmpty() {
    registerUDFV2("ST_IsEmpty", String.class);
    verifySqlSingleRes("select sedona.ST_IsEmpty(ST_GeometryFromWKT('POINT(1 2)'))", false);
  }

  @Test
  public void test_ST_IsRing() {
    registerUDFV2("ST_IsRing", String.class);
    verifySqlSingleRes(
        "select sedona.ST_IsRing(ST_GeometryFromWKT('LINESTRING(0 0, 2 2)'))", false);
    verifySqlSingleRes(
        "select sedona.ST_IsRing(ST_GeometryFromWKT('LINESTRING(0 0, 2 2, 1 2, 0 0)'))", true);
  }

  @Test
  public void test_ST_IsSimple() {
    registerUDFV2("ST_IsSimple", String.class);
    verifySqlSingleRes(
        "select sedona.ST_IsSimple(ST_GeometryFromWKT('LINESTRING(0 0, 2 2)'))", true);
    verifySqlSingleRes(
        "select sedona.ST_IsSimple(ST_GeometryFromWKT('POLYGON((1 1,3 1,3 3,2 0,1 1))', 4326, TRUE))",
        false);
  }

  @Test
  public void test_ST_IsValid() {
    registerUDFV2("ST_IsValid", String.class);
    verifySqlSingleRes(
        "select sedona.ST_IsValid(ST_GeometryFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))', 4326, TRUE))",
        false);
    registerUDFV2("ST_IsValid", String.class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_IsValid(ST_GeometryFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))', 4326, TRUE), 1)",
        false);
  }

  @Test
  public void test_ST_IsValidReason() {
    registerUDFV2("ST_IsValidReason", String.class);
    verifySqlSingleRes(
        "select sedona.ST_IsValidReason(ST_GeometryFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))', 4326, TRUE))",
        "Hole lies outside shell at or near point (15.0, 15.0, NaN)");
    registerUDFV2("ST_IsValidReason", String.class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_IsValidReason(ST_GeometryFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))', 4326, TRUE), 1)",
        "Hole lies outside shell at or near point (15.0, 15.0, NaN)");
  }

  @Test
  public void test_ST_Length() {
    registerUDFV2("ST_Length", String.class);
    verifySqlSingleRes(
        "select sedona.ST_Length(ST_GeometryFromWKT('LINESTRING(0 0, 2 2)'))", 2.8284271247461903);
  }

  @Test
  public void test_ST_Length2D() {
    registerUDFV2("ST_Length2D", String.class);
    verifySqlSingleRes(
        "select sedona.ST_Length2D(ST_GeometryFromWKT('LINESTRING(0 0, 2 2)'))",
        2.8284271247461903);
  }

  @Test
  public void test_ST_LineFromMultiPoint() {
    registerUDFV2("ST_LineFromMultiPoint", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_LineFromMultiPoint(ST_GeometryFromWKT('MULTIPOINT((10 40), (40 30), (20 20), (30 10))')))",
        "LINESTRING(10 40,40 30,20 20,30 10)");
  }

  @Test
  public void test_ST_LineInterpolatePoint() {
    registerUDFV2("ST_LineInterpolatePoint", String.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_LineInterpolatePoint(ST_GeometryFromWKT('LINESTRING(25 50, 100 125, 150 190)'), 0.2))",
        "POINT(51.597413505 76.597413505)");
  }

  @Test
  public void test_ST_LineLocatePoint() {
    registerUDFV2("ST_LineLocatePoint", String.class, String.class);
    verifySqlSingleRes(
        "select sedona.ST_LineLocatePoint(ST_GeometryFromWKT('LINESTRING(0 0, 10 10)'), ST_GeometryFromWKT('POINT(2 2)'))",
        0.2);
  }

  @Test
  public void test_ST_LineMerge() {
    registerUDFV2("ST_LineMerge", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_LineMerge(ST_GeometryFromWKT('MULTILINESTRING((0 0, 1 1), (1 1, 2 2))')))",
        "LINESTRING(0 0,1 1,2 2)");
  }

  @Test
  public void test_ST_LineSubstring() {
    registerUDFV2("ST_LineSubstring", String.class, double.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_LineSubstring(ST_GeometryFromWKT('LINESTRING (20 180, 50 20, 90 80, 120 40, 180 150)'), 0.333, 0.666))",
        "LINESTRING(45.173118104 45.743370112,50 20,90 80,112.975930502 49.365425998)");
  }

  @Test
  public void test_ST_LongestLine() {
    registerUDFV2("ST_LongestLine", String.class, String.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_LongestLine(ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))'), ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))')))",
        "LINESTRING(180 180,20 50)");
  }

  @Test
  public void test_ST_MakePolygon() {
    registerUDFV2("ST_MakePolygon", String.class);
    registerUDFV2("ST_MakePolygon", String.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_MakePolygon(ST_GeometryFromWKT('LINESTRING(7 -1, 7 6, 9 6, 9 1, 7 -1)', 4326, TRUE)))",
        "POLYGON((7 -1,7 6,9 6,9 1,7 -1))");
  }

  @Test
  public void test_ST_MakeValid() {
    registerUDFV2("ST_MakeValid", String.class);
    registerUDFV2("ST_MakeValid", String.class, boolean.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_MakeValid(ST_GeometryFromWKT('POLYGON((1 5, 1 1, 3 3, 5 3, 7 1, 7 5, 5 3, 3 3, 1 5))', 4326, TRUE)))",
        "MULTIPOLYGON(((1 5,3 3,1 1,1 5)),((5 3,7 5,7 1,5 3)))");
  }

  @Test
  public void test_ST_MaxDistance() {
    registerUDFV2("ST_MaxDistance", String.class, String.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_MaxDistance(ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))'), ST_GeomFromWKT('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 99 77.5, 90 140, 60 140))'))",
        206.15528128088303);
  }

  @Test
  public void test_ST_MinimumClearance() {
    registerUDFV2("ST_MinimumClearance", String.class);
    verifySqlSingleRes(
        "select sedona.ST_MinimumClearance(ST_GeomFromText('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))'))",
        0.5);
  }

  @Test
  public void test_ST_MinimumClearanceLine() {
    registerUDFV2("ST_MinimumClearanceLine", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_MinimumClearanceLine(ST_GeomFromText('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))')))",
        "LINESTRING(64.5 16,65 16)");
  }

  @Test
  public void test_ST_MinimumBoundingCircle() {
    registerUDFV2("ST_MinimumBoundingCircle", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_MinimumBoundingCircle(ST_GeometryFromWKT('GeometryCollection (LINESTRING(55 75,125 150), POINT (20 80))') , 8))",
        "POLYGON((135.597147321 115,134.384753327 102.690357211,130.794162969 90.853767091,124.96336062 79.94510316,117.116420744 70.383579256,107.55489684 62.53663938,96.646232909 56.705837031,84.809642789 53.115246673,72.5 51.902852679,60.190357211 53.115246673,48.353767091 56.705837031,37.44510316 62.53663938,27.883579256 70.383579256,20.03663938 79.94510316,14.205837031 90.853767091,10.615246673 102.690357211,9.402852679 115,10.615246673 127.309642789,14.205837031 139.146232909,20.03663938 150.05489684,27.883579256 159.616420744,37.44510316 167.46336062,48.353767091 173.294162969,60.190357211 176.884753327,72.5 178.097147321,84.809642789 176.884753327,96.646232909 173.294162969,107.55489684 167.46336062,117.116420744 159.616420744,124.96336062 150.05489684,130.794162969 139.146232909,134.384753327 127.309642789,135.597147321 115))");
  }

  @Test
  public void test_ST_Multi() {
    registerUDFV2("ST_Multi", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Multi(ST_GeometryFromWKT('POINT(1 2)')))", "MULTIPOINT((1 2))");
  }

  @Test
  public void test_ST_NDims() {
    registerUDFV2("ST_NDims", String.class);
    verifySqlSingleRes("select sedona.ST_NDims(ST_GeometryFromWKT('POINT(1 1)'))", 2);
  }

  @Test
  public void test_ST_Normalize() {
    registerUDFV2("ST_Normalize", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Normalize(ST_GeometryFromWKT('POLYGON((0 1, 1 1, 1 0, 0 0, 0 1))')))",
        "POLYGON((0 0,0 1,1 1,1 0,0 0))");
  }

  @Test
  public void test_ST_NPoints() {
    registerUDFV2("ST_NPoints", String.class);
    verifySqlSingleRes(
        "select sedona.ST_NPoints(ST_GeometryFromWKT('LINESTRING(1 2, 3 4, 5 6)'))", 3);
  }

  @Test
  public void test_ST_NumGeometries() {
    registerUDFV2("ST_NumGeometries", String.class);
    verifySqlSingleRes(
        "select sedona.ST_NumGeometries(ST_GeometryFromWKT('GEOMETRYCOLLECTION(POINT(1 2), POINT(3 4), LINESTRING(1 1, 1 2))'))",
        3);
  }

  @Test
  public void test_ST_NumInteriorRings() {
    registerUDFV2("ST_NumInteriorRings", String.class);
    verifySqlSingleRes(
        "select sedona.ST_NumInteriorRings(ST_GeometryFromWKT('POLYGON((0 0,0 5,5 0,0 0),(1 1,3 1,1 3,1 1))'))",
        1);
    verifySqlSingleRes(
        "select sedona.ST_NumInteriorRings(ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        0);
  }

  @Test
  public void test_ST_NumInteriorRing() {
    registerUDFV2("ST_NumInteriorRing", String.class);
    verifySqlSingleRes(
        "select sedona.ST_NumInteriorRing(ST_GeometryFromWKT('POLYGON((0 0,0 5,5 0,0 0),(1 1,3 1,1 3,1 1))'))",
        1);
    verifySqlSingleRes(
        "select sedona.ST_NumInteriorRing(ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        0);
  }

  @Test
  public void test_ST_PointN() {
    registerUDFV2("ST_PointN", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_PointN(ST_GeometryFromWKT('LINESTRING(1 2, 3 4, 5 6)'), 2))",
        "POINT(3 4)");
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_PointN(ST_GeometryFromWKT('LINESTRING(1 2, 3 4, 5 6)'), -1))",
        "POINT(5 6)");
  }

  @Test
  public void test_ST_PointOnSurface() {
    registerUDFV2("ST_PointOnSurface", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_PointOnSurface(ST_GeometryFromWKT('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')))",
        "POINT(2.5 2.5)");
  }

  @Test
  public void test_ST_Points() {
    registerUDFV2("ST_Points", String.class);
    verifySqlSingleRes(
        "select ST_AsEWKT(sedona.ST_Points(ST_GeometryFromWKT('LINESTRING(0 0, 0 1, 0 2, 0 3, 0 4)')))",
        "SRID=0;MULTIPOINT((0 0),(0 1),(0 2),(0 3),(0 4))");
  }

  @Test
  public void test_ST_Polygon() {
    registerUDFV2("ST_Polygon", String.class, int.class);
    // GeoJSON spec does not contain SRID so the serialization process will lose SRID info
    verifySqlSingleRes(
        "select ST_AsEWKT(sedona.ST_Polygon(ST_GeometryFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'), 4326))",
        "SRID=0;POLYGON((0 0,0 1,1 1,1 0,0 0))");
  }

  @Test
  public void test_ST_Polygonize() {
    registerUDFV2("ST_Polygonize", String.class);
    registerUDFV2("ST_Area", String.class);
    verifySqlSingleRes(
        "select ST_Area(sedona.ST_Polygonize(ST_GeometryFromWKT('GEOMETRYCOLLECTION (LINESTRING (2 0, 2 1, 2 2), LINESTRING (2 2, 2 3, 2 4), LINESTRING (0 2, 1 2, 2 2), LINESTRING (2 2, 3 2, 4 2), LINESTRING (0 2, 1 3, 2 4), LINESTRING (2 4, 3 3, 4 2))')))",
        4.0);
  }

  @Test
  public void test_ST_PrecisionReduce() {
    registerUDFV2("ST_PrecisionReduce", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_PrecisionReduce(ST_GeometryFromWKT('POINT(1.123456789 2.123456789)'), 3))",
        "POINT(1.123 2.123)");
  }

  @Test
  public void test_ST_ReducePrecision() {
    registerUDFV2("ST_ReducePrecision", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_ReducePrecision(ST_GeometryFromWKT('POINT(1.123456789 2.123456789)'), 3))",
        "POINT(1.123 2.123)");
  }

  @Test
  public void test_ST_RemovePoint() {
    registerUDFV2("ST_RemovePoint", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_RemovePoint(ST_GeometryFromWKT('LINESTRING(1 2, 3 4, 5 6)')))",
        "LINESTRING(1 2,3 4)");
    registerUDFV2("ST_RemovePoint", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_RemovePoint(ST_GeometryFromWKT('LINESTRING(1 2, 3 4, 5 6)'), 1))",
        "LINESTRING(1 2,5 6)");
  }

  @Test
  public void test_ST_Reverse() {
    registerUDFV2("ST_Reverse", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Reverse(ST_GeometryFromWKT('LINESTRING(1 2, 3 4, 5 6)')))",
        "LINESTRING(5 6,3 4,1 2)");
  }

  @Test
  public void test_ST_S2CellIDs() {
    registerUDFV2("ST_S2CellIDs", String.class, int.class);
    verifySqlSingleRes(
        "select sedona.ST_S2CellIDs(ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), 9)",
        "[\n  1153031455769624576,\n  1152961087025446912,\n  1152925902653358080,\n  1152934698746380288,\n  1152943494839402496,\n  1152952290932424704,\n  1152969883118469120,\n  1152978679211491328,\n  1152987475304513536,\n  1152996271397535744,\n  1153005067490557952,\n  1153049047955668992,\n  1153057844048691200,\n  1153040251862646784,\n  1153084232327757824,\n  1153093028420780032,\n  1153066640141713408,\n  1153075436234735616,\n  1153101824513802240,\n  1153137008885891072,\n  1153189785444024320,\n  1153198581537046528,\n  1153172193257979904,\n  1153180989351002112,\n  1153163397164957696,\n  1153128212792868864,\n  1153013863583580160,\n  1153022659676602368,\n  1153242562002157568,\n  1153216173723090944,\n  1153277746374246400,\n  1153207377630068736,\n  1153224969816113152,\n  1153233765909135360,\n  1153268950281224192,\n  1153321726839357440,\n  1153365707304468480,\n  1153374503397490688,\n  1153400891676557312,\n  1153409687769579520,\n  1153383299490512896,\n  1153392095583535104,\n  1153436076048646144,\n  1153444872141668352,\n  1153418483862601728,\n  1153427279955623936,\n  1153453668234690560,\n  1153462464327712768,\n  1153330522932379648,\n  1921361385166471168,\n  1921475734375759872,\n  1921484530468782080,\n  1921493326561804288,\n  1921519714840870912,\n  1921528510933893120,\n  1921537307026915328,\n  384305702186778624,\n  1152389340979003392,\n  1152398137072025600,\n  1152406933165047808,\n  1152873126095224832,\n  1152881922188247040,\n  1152890718281269248,\n  1152917106560335872\n]");
  }

  @Test
  public void test_ST_SetPoint() {
    registerUDFV2("ST_SetPoint", String.class, int.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_SetPoint(ST_GeometryFromWKT('LINESTRING(1 2, 3 4, 5 6)'), 1, ST_GeometryFromWKT('POINT(10 10)')))",
        "LINESTRING(1 2,10 10,5 6)");
  }

  /**
   * Test method to test the ST_SetSRID function. Note that Snowflake GeoJSON serializer does not
   * support SRID. So the result is always SRID=0.
   */
  @Test
  public void test_ST_SetSRID() {
    registerUDFV2("ST_AsEWKT", String.class);
    registerUDFV2("ST_SetSRID", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsEWKT(sedona.ST_SetSRID(ST_GeometryFromWKT('POINT(1 2)'), 4326))",
        "SRID=0;POINT(1 2)");
  }

  @Test
  public void test_ST_SimplifyPreserveTopology() {
    registerUDFV2("ST_SimplifyPreserveTopology", String.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_SimplifyPreserveTopology(ST_GeometryFromWKT('POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33,46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22,35 26, 24 39, 8 25))'), 10))",
        "POLYGON((8 25,28 22,15 11,33 3,56 30,47 44,35 36,43 19,24 39,8 25))");
  }

  @Test
  public void test_ST_SimplifyVW() {
    registerUDFV2("ST_SimplifyVW", String.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_SimplifyVW(ST_GeometryFromWKT('POLYGON((8 25, 28 22, 28 20, 15 11, 33 3, 56 30, 46 33,46 34, 47 44, 35 36, 45 33, 43 19, 29 21, 29 22,35 26, 24 39, 8 25))'), 10))",
        "POLYGON((8 25,28 22,28 20,15 11,33 3,56 30,46 33,47 44,35 36,45 33,43 19,29 21,35 26,24 39,8 25))");
  }

  @Test
  public void test_ST_SimplifyPolygonHull() {
    registerUDFV2("ST_SimplifyPolygonHull", String.class, double.class, boolean.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_SimplifyPolygonHull(ST_GeomFromText('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))'), 0.3, false))",
        "POLYGON((30 10,40 40,10 20,30 10))");
    registerUDFV2("ST_SimplifyPolygonHull", String.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_SimplifyPolygonHull(ST_GeomFromText('POLYGON ((30 10, 40 40, 45 45, 20 40, 25 35, 10 20, 15 15, 30 10))'), 0.3))",
        "POLYGON((30 10,15 15,10 20,20 40,45 45,30 10))");
  }

  @Test
  public void test_ST_Split() {
    registerUDFV2("ST_Split", String.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Split(ST_GeometryFromWKT('LINESTRING (0 0, 1.5 1.5, 2 2)'), ST_GeometryFromWKT('MULTIPOINT (0.5 0.5, 1 1)')))",
        "MULTILINESTRING((0 0,0.5 0.5),(0.5 0.5,1 1),(1 1,1.5 1.5,2 2))");
  }

  @Test
  public void test_ST_SRID() {
    registerUDFV2("ST_SRID", String.class);
    verifySqlSingleRes("select sedona.ST_SRID(ST_GeometryFromWKT('POINT(1 2)'))", 0);
  }

  @Test
  public void test_ST_StartPoint() {
    registerUDFV2("ST_StartPoint", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_StartPoint(ST_GeometryFromWKT('LINESTRING(1 2, 3 4, 5 6)')))",
        "POINT(1 2)");
  }

  @Test
  public void test_ST_Snap() {
    registerUDFV2("ST_Snap", String.class, String.class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(sedona.ST_Snap(ST_GeomFromWKT('POLYGON((2.6 12.5, 2.6 20.0, 12.6 20.0, 12.6 12.5, 2.6 12.5 ))'), ST_GeomFromWKT('LINESTRING (0.5 10.7, 5.4 8.4, 10.1 10.0)'), 2.525))",
        "POLYGON ((2.6 12.5, 2.6 20, 12.6 20, 12.6 12.5, 10.1 10, 2.6 12.5))");
  }

  @Test
  public void test_ST_SubDivide() {
    registerUDFV2("ST_SubDivide", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_SubDivide(ST_GeometryFromWKT('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)'), 5))",
        "MULTILINESTRING((0 0,2.5 0),(2.5 0,5 0))");
  }

  @Test
  public void test_ST_SymDifference() {
    registerUDFV2("ST_SymDifference", String.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_SymDifference(ST_GeometryFromWKT('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))'), ST_GeometryFromWKT('POLYGON ((0 -2, 2 -2, 2 0, 0 0, 0 -2))')))",
        "MULTIPOLYGON(((0 -1,-1 -1,-1 1,1 1,1 0,0 0,0 -1)),((0 -1,1 -1,1 0,2 0,2 -2,0 -2,0 -1)))");
  }

  @Test
  public void test_ST_Transform() {
    registerUDFV2("ST_Transform", String.class, String.class, String.class, boolean.class);
    verifySqlSingleRes(
        "select ST_AsText(SEDONA.ST_Transform(ST_geomFromWKT('POLYGON ((110.54671 55.818002, 110.54671 55.143743, 110.940494 55.143743, 110.940494 55.818002, 110.54671 55.818002))'),'EPSG:4326', 'EPSG:32649', false))",
        "POLYGON((471596.691674602 6185916.95119129,471107.562364101 6110880.97422817,496207.109151055 6110788.80471244,496271.319370462 6185825.60569904,471596.691674602 6185916.95119129))");
  }

  @Test
  public void test_ST_Union() {
    registerUDFV2("ST_Union", String.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_Union(ST_GeometryFromWKT('POLYGON ((-3 -3, 3 -3, 3 3, -3 3, -3 -3))'), ST_GeometryFromWKT('POLYGON ((-2 1, 2 1, 2 4, -2 4, -2 1))')))",
        "POLYGON((2 3,3 3,3 -3,-3 -3,-3 3,-2 3,-2 4,2 4,2 3))");
  }

  @Test
  public void test_ST_UnaryUnion() {
    registerUDFV2("ST_UnaryUnion", String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_UnaryUnion(ST_GeometryFromWKT('MULTILINESTRING ((10 10, 20 20, 30 30),(25 25, 35 35, 45 45),(40 40, 50 50, 60 60),(55 55, 65 65, 75 75))')))",
        "MULTILINESTRING((10 10,20 20,25 25),(25 25,30 30),(30 30,35 35,40 40),(40 40,45 45),(45 45,50 50,55 55),(55 55,60 60),(60 60,65 65,75 75))");
  }

  @Test
  public void test_ST_VoronoiPolygons() {
    registerUDFV2("ST_VoronoiPolygons", String.class);
    registerUDFV2("ST_VoronoiPolygons", String.class, double.class);
    registerUDFV2("ST_VoronoiPolygons", String.class, double.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_VoronoiPolygons(ST_GeometryFromWKT('MULTIPOINT ((0 0), (1 1))')))",
        "GEOMETRYCOLLECTION(POLYGON((-1 -1,-1 2,2 -1,-1 -1)),POLYGON((-1 2,2 2,2 -1,-1 2)))");
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_VoronoiPolygons(ST_GeometryFromWKT('MULTIPOINT ((0 0), (1 1))'), 1))",
        "GEOMETRYCOLLECTION(POLYGON((-1 -1,-1 2,2 -1,-1 -1)),POLYGON((-1 2,2 2,2 -1,-1 2)))");
    verifySqlSingleRes(
        "select ST_AsText(sedona.ST_VoronoiPolygons(ST_GeometryFromWKT('MULTIPOINT ((0 0), (1 1))'), 1, ST_GeometryFromWKT('POLYGON ((-1 -1, -1 2, 2 -1, -1 -1))')))",
        "GEOMETRYCOLLECTION(POLYGON((-1 -1,-1 2,2 -1,-1 -1)),POLYGON((-1 2,2 2,2 -1,-1 2)))");
  }

  @Test
  public void test_ST_X() {
    registerUDFV2("ST_X", String.class);
    verifySqlSingleRes("select sedona.ST_X(ST_GeometryFromWKT('POINT(1 2)'))", 1.0);
    verifySqlSingleRes("select sedona.ST_X(ST_GeometryFromWKT('LINESTRING(1 2, 2 2)'))", null);
  }

  @Test
  public void test_ST_XMax() {
    registerUDFV2("ST_XMax", String.class);
    verifySqlSingleRes(
        "select sedona.ST_XMax(ST_GeometryFromWKT('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))",
        2.0);
  }

  @Test
  public void test_ST_XMin() {
    registerUDFV2("ST_XMin", String.class);
    verifySqlSingleRes(
        "select sedona.ST_XMin(ST_GeometryFromWKT('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))",
        -1.0);
  }

  @Test
  public void test_ST_Y() {
    registerUDFV2("ST_Y", String.class);
    verifySqlSingleRes("select sedona.ST_Y(ST_GeometryFromWKT('POINT(1 2)'))", 2.0);
    verifySqlSingleRes(
        "select sedona.ST_Y(ST_GeometryFromWKT('LINESTRING(1 -1, 2 2, 2 3)'))", null);
  }

  @Test
  public void test_ST_YMax() {
    registerUDFV2("ST_YMax", String.class);
    verifySqlSingleRes(
        "select sedona.ST_YMax(ST_GeometryFromWKT('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))",
        12.0);
  }

  @Test
  public void test_ST_YMin() {
    registerUDFV2("ST_YMin", String.class);
    verifySqlSingleRes(
        "select sedona.ST_YMin(ST_GeometryFromWKT('POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))'))",
        -11.0);
  }

  @Test
  public void test_ST_Z() {
    registerUDFV2("ST_Z", String.class);
    verifySqlSingleRes("select sedona.ST_Z(ST_GeometryFromWKT('POINT Z(1 2 3)'))", 3.0);
    verifySqlSingleRes(
        "select sedona.ST_Z(ST_GeometryFromWKT('LINESTRING Z(1 -1 1, 2 2 2, 2 3 3)'))", null);
  }

  @Test
  public void test_ST_ZMax() {
    registerUDFV2("ST_ZMax", String.class);
    verifySqlSingleRes(
        "select sedona.ST_ZMax(ST_GeometryFromWKT('POLYGON Z((-1 -11 1, 0 10 2, 1 11 3, 2 12 4, -1 -11 5))'))",
        5.0);
  }

  @Test
  public void test_ST_ZMin() {
    registerUDFV2("ST_ZMin", String.class);
    verifySqlSingleRes(
        "select sedona.ST_ZMin(ST_GeometryFromWKT('POLYGON Z((-1 -11 1, 0 10 2, 1 11 3, 2 12 4, -1 -11 5))'))",
        1.0);
  }

  @Test
  public void test_ST_AreaSpheroid() {
    registerUDFV2("ST_AreaSpheroid", String.class);
    verifySqlSingleRes(
        "select sedona.ST_AreaSpheroid(ST_GeometryFromWKT('Polygon ((34 35, 28 30, 25 34, 34 35))'))",
        201824850811.76245);
  }

  @Test
  public void test_ST_DistanceSphere() {
    registerUDFV2("ST_DistanceSphere", String.class, String.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_DistanceSphere(ST_GeomFromWKT('POINT (-0.56 51.3168)'), ST_GeomFromWKT('POINT (-3.1883 55.9533)'))",
        543796.9506134904);
    registerUDFV2("ST_DistanceSphere", String.class, String.class, double.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_DistanceSphere(ST_GeomFromWKT('POINT (-0.56 51.3168)'), ST_GeomFromWKT('POINT (-3.1883 55.9533)'), 6378137.0)",
        544405.4459192449);
  }

  @Test
  public void test_ST_DistanceSpheroid() {
    registerUDFV2("ST_DistanceSpheroid", String.class, String.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_DistanceSpheroid(ST_GeomFromWKT('POINT (-0.56 51.3168)'), ST_GeomFromWKT('POINT (-3.1883 55.9533)'))",
        544430.9411996207);
  }

  @Test
  public void test_ST_FrechetDistance() {
    registerUDFV2("ST_FrechetDistance", String.class, String.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_FrechetDistance(ST_GeomFromWKT('LINESTRING (0 0, 1 1, 2 2)'), ST_GeomFromWKT('LINESTRING (0 0, 1 1, 3 3)'))",
        1.4142135623730951);
  }

  @Test
  public void test_ST_Force3D() {
    registerUDFV2("ST_Force3D", String.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_Force3D(ST_GeometryFromWKT('LINESTRING(0 1, 1 2, 2 1)')))",
        "LINESTRINGZ(0 1 0,1 2 0,2 1 0)");
    registerUDFV2("ST_Force3D", String.class, double.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_Force3D(ST_GeometryFromWKT('LINESTRING(0 1, 1 2, 2 1)'), 1))",
        "LINESTRINGZ(0 1 1,1 2 1,2 1 1)");
  }

  @Test
  public void test_ST_Force3DZ() {
    registerUDFV2("ST_Force3DZ", String.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_Force3DZ(ST_GeometryFromWKT('LINESTRING(0 1, 1 2, 2 1)')))",
        "LINESTRINGZ(0 1 0,1 2 0,2 1 0)");
    registerUDFV2("ST_Force3DZ", String.class, double.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_Force3DZ(ST_GeometryFromWKT('LINESTRING(0 1, 1 2, 2 1)'), 1))",
        "LINESTRINGZ(0 1 1,1 2 1,2 1 1)");
  }

  @Test
  public void test_ST_ForceCollection() {
    registerUDFV2("ST_ForceCollection", String.class);
    registerUDFV2("ST_NumGeometries", String.class);
    verifySqlSingleRes(
        "SELECT ST_NumGeometries(sedona.ST_ForceCollection(ST_GeomFromWKT('MULTIPOINT (30 10, 40 40, 20 20, 10 30, 10 10, 20 50)')))",
        6);
  }

  @Test
  public void test_ST_ForcePolygonCW() {
    registerUDFV2("ST_ForcePolygonCW", String.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_ForcePolygonCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')))",
        "POLYGON((20 35,45 20,30 5,10 10,10 30,20 35),(30 20,20 25,20 15,30 20))");
  }

  @Test
  public void test_ST_IsPolygonCW() {
    registerUDFV2("ST_IsPolygonCW", String.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_IsPolygonCW(ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))'))",
        true);
  }

  @Test
  public void test_ST_ForceRHR() {
    registerUDFV2("ST_ForceRHR", String.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_ForceRHR(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')))",
        "POLYGON((20 35,45 20,30 5,10 10,10 30,20 35),(30 20,20 25,20 15,30 20))");
  }

  @Test
  public void test_ST_LengthSpheroid() {
    registerUDFV2("ST_LengthSpheroid", String.class);
    verifySqlSingleRes(
        "select sedona.ST_LengthSpheroid(ST_GeomFromWKT('Polygon ((0 0, 90 0, 90 90, 0 90, 0 0))'))",
        30022685.630020067);
  }

  @Test
  public void test_ST_ForcePolygonCCW() {
    registerUDFV2("ST_ForcePolygonCCW", String.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_ForcePolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))')))",
        "POLYGON((20 35,10 30,10 10,30 5,45 20,20 35),(30 20,20 15,20 25,30 20))");
  }

  @Test
  public void test_ST_IsPolygonCCW() {
    registerUDFV2("ST_IsPolygonCCW", String.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_IsPolygonCCW(ST_GeomFromWKT('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))'))",
        true);
  }

  @Test
  public void test_ST_GeometricMedian() {
    registerUDFV2("ST_GeometricMedian", String.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_GeometricMedian(ST_GeomFromWKT('MULTIPOINT((0 0), (1 1), (2 2), (200 200))')))",
        "POINT(1.976155028 1.976155028)");
    registerUDFV2("ST_GeometricMedian", String.class, float.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_GeometricMedian(ST_GeomFromWKT('MULTIPOINT ((0 -1), (0 0), (0 0), (0 1))'), 1e-6))",
        "POINT(0 0)");
  }

  @Test
  public void test_ST_NRings() {
    registerUDFV2("ST_NRings", String.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_NRings(ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))", 1);
  }

  @Test
  public void test_ST_NumPoints() {
    registerUDFV2("ST_NumPoints", String.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_NumPoints(ST_GeometryFromWKT('LINESTRING(0 0, 1 1, 2 2)'))", 3);
  }

  @Test
  public void test_ST_TriangulatePolygon() {
    registerUDFV2("ST_TriangulatePolygon", String.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_TriangulatePolygon(ST_GeomFromWKT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (5 5, 5 8, 8 8, 8 5, 5 5))')))",
        "GEOMETRYCOLLECTION(POLYGON((0 0,0 10,5 5,0 0)),POLYGON((5 8,5 5,0 10,5 8)),POLYGON((10 0,0 0,5 5,10 0)),POLYGON((10 10,5 8,0 10,10 10)),POLYGON((10 0,5 5,8 5,10 0)),POLYGON((5 8,10 10,8 8,5 8)),POLYGON((10 10,10 0,8 5,10 10)),POLYGON((8 5,8 8,10 10,8 5)))");
  }

  @Test
  public void test_ST_Translate() {
    registerUDFV2("ST_Translate", String.class, double.class, double.class);
    verifySqlSingleRes(
        "SELECT ST_AsText(sedona.ST_Translate(ST_GeometryFromWKT('POINT(1 3)'), 1, 2))",
        "POINT(2 5)");
  }

  @Test
  public void test_ST_RotateX() {
    registerUDFV2("ST_RotateX", String.class, double.class);
    registerUDFV2("ST_ReducePrecision", String.class, int.class);
    verifySqlSingleRes(
        "select ST_AsText(ST_ReducePrecision(sedona.ST_RotateX(ST_GeometryFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)'), 10),2))",
        "LINESTRING(0 0,1 0,1 -0.84,0 0)");
  }

  @Test
  public void test_ST_Rotate() {
    registerUDFV2("ST_Rotate", String.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(ST_ReducePrecision(sedona.ST_Rotate(ST_GeometryFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)'), 10),2))",
        "LINESTRING(0 0,-0.84 -0.54,-0.3 -1.38,0 0)");
    registerUDFV2("ST_Rotate", String.class, double.class, String.class);
    verifySqlSingleRes(
        "select ST_AsText(ST_ReducePrecision(sedona.ST_Rotate(ST_GeometryFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)'), 10, ST_GeometryFromWKT('POINT (0 0)')),2))",
        "LINESTRING(0 0,-0.84 -0.54,-0.3 -1.38,0 0)");
    registerUDFV2("ST_Rotate", String.class, double.class, double.class, double.class);
    verifySqlSingleRes(
        "select ST_AsText(ST_ReducePrecision(sedona.ST_Rotate(ST_GeometryFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 0)'), 10, 0, 0),2))",
        "LINESTRING(0 0,-0.84 -0.54,-0.3 -1.38,0 0)");
  }
}
