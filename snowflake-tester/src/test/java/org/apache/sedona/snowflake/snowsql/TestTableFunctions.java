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

import java.util.Arrays;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.snowflake.snowsql.udtfs.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.jts.io.ParseException;

@RunWith(SnowTestRunner.class)
public class TestTableFunctions extends TestBase {
  @Test
  public void test_ST_MinimumBoundingRadius() {
    registerUDTF(ST_MinimumBoundingRadius.class);
    verifySqlSingleRes(
        "SELECT sedona.ST_AsText(center), radius from table(sedona.ST_MinimumBoundingRadius(sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')))",
        Arrays.asList("POINT (0.5 0.5)", 0.7071067811865476));
  }

  @Test
  public void test_ST_Intersection_Aggr() throws ParseException {
    registerUDTF(ST_Intersection_Aggr.class);
    verifySqlSingleRes(
        "with src_tbl as (\n"
            + "select sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') geom\n"
            + "union\n"
            + "select sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') geom\n"
            + ")\n"
            + "select sedona.ST_AsText(intersected) from src_tbl, table(sedona.ST_Intersection_Aggr(src_tbl.geom) OVER (PARTITION BY 1));",
        Constructors.geomFromWKT("POLYGON ((0.5 1, 1 1, 1 0.5, 0.5 0.5, 0.5 1))", 0));
  }

  @Test
  public void test_ST_MaximumInscribedCircle() {
    registerUDTF(ST_MaximumInscribedCircle.class);
    verifySqlSingleRes(
        "select sedona.ST_AsText(center) from table(sedona.ST_MaximumInscribedCircle(sedona.ST_GeomFromText('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 50 90, 90 140, 60 140))')))",
        "POINT (96.953125 76.328125)");
    verifySqlSingleRes(
        "select sedona.ST_AsText(nearest) from table(sedona.ST_MaximumInscribedCircle(sedona.ST_GeomFromText('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 50 90, 90 140, 60 140))')))",
        "POINT (140 90)");
    verifySqlSingleRes(
        "select radius from table(sedona.ST_MaximumInscribedCircle(sedona.ST_GeomFromText('POLYGON ((40 180, 110 160, 180 180, 180 120, 140 90, 160 40, 80 10, 70 40, 20 50, 40 180),(60 140, 50 90, 90 140, 60 140))')))",
        45.165845650018);
  }

  @Test
  public void test_ST_IsValidDetail() {
    registerUDTF(ST_IsValidDetail.class);
    verifySqlSingleRes(
        "select reason from table(sedona.ST_IsValidDetail(sedona.ST_GeomFromText('POLYGON ((30 10, 40 40, 20 40, 30 10, 10 20, 30 10))'), 0))",
        "Ring Self-intersection at or near point (30.0, 10.0, NaN)");
    verifySqlSingleRes(
        "select valid from table(sedona.ST_IsValidDetail(sedona.ST_GeomFromText('POLYGON ((30 10, 40 40, 20 40, 30 10, 10 20, 30 10))'), 0))",
        false);
    verifySqlSingleRes(
        "select sedona.ST_AsText(location) from table(sedona.ST_IsValidDetail(sedona.ST_GeomFromText('POLYGON ((30 10, 40 40, 20 40, 30 10, 10 20, 30 10))'), 0))",
        "POINT (30 10)");
  }

  @Test
  public void test_ST_SubDivideExplode() {
    registerUDTF(ST_SubDivideExplode.class);
    verifySqlSingleRes(
        "select count(1) from table(sedona.ST_SubdivideExplode(sedona.ST_GeomFromText('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)'), 5));",
        2);
  }

  @Test
  public void test_ST_Envelope_Aggr() throws ParseException {
    registerUDTF(ST_Envelope_Aggr.class);
    verifySqlSingleRes(
        "with src_tbl as (\n"
            + "select sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') geom\n"
            + "union\n"
            + "select sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') geom\n"
            + ")\n"
            + "select sedona.ST_AsText(envelope) from src_tbl, table(sedona.ST_Envelope_Aggr(src_tbl.geom) OVER (PARTITION BY 1));",
        Constructors.geomFromWKT("POLYGON ((0 0, 0 1.5, 1.5 1.5, 1.5 0, 0 0))", 0));
  }

  @Test
  public void test_ST_Union_Aggr() throws ParseException {
    registerUDTF(ST_Union_Aggr.class);
    verifySqlSingleRes(
        "with src_tbl as (\n"
            + "select sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') geom\n"
            + "union\n"
            + "select sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') geom\n"
            + ")\n"
            + "select sedona.ST_AsText(unioned) from src_tbl, table(sedona.ST_Union_Aggr(src_tbl.geom) OVER (PARTITION BY 1));",
        Constructors.geomFromWKT(
            "POLYGON ((0 0, 0 1, 0.5 1, 0.5 1.5, 1.5 1.5, 1.5 0.5, 1 0.5, 1 0, 0 0))", 0));
  }

  @Test
  public void test_ST_Collect() throws ParseException {
    registerUDTF(ST_Collect.class);
    verifySqlSingleRes(
        "with src_tbl as (\n"
            + "select sedona.ST_GeomFromText('POINT (40 10)') geom\n"
            + "union\n"
            + "select sedona.ST_GeomFromText('LINESTRING (0 5, 0 10)') geom\n"
            + ")\n"
            + "select sedona.ST_AsText(collection) from src_tbl, table(sedona.ST_Collect(src_tbl.geom) OVER (PARTITION BY 1));",
        Constructors.geomFromWKT("GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (0 5, 0 10))", 0));
  }

  @Test
  public void test_ST_DumpExplode() {
    registerUDTF(ST_Dump.class);
    verifySqlSingleRes(
        "select count(1) from table(sedona.ST_Dump(sedona.ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')));",
        4);
  }
}
