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

@RunWith(SnowTestRunner.class)
public class TestPredicatesV2 extends TestBase {

  public void test_ST_Contains() {
    registerUDFV2("ST_Contains", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CONTAINS( ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),  ST_GeometryFromWKT('POINT(0.5 0.5)'))",
        true);
  }

  @Test
  public void test_ST_Crosses() {
    registerUDFV2("ST_Crosses", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Crosses( ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),  ST_GeometryFromWKT('LINESTRING(0.5 0.5, 1.1 0.5)'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Crosses( ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),  ST_GeometryFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        false);
  }

  @Test
  public void test_ST_Disjoint() {
    registerUDFV2("ST_Disjoint", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Disjoint( ST_GeometryFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'),  ST_GeometryFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Disjoint( ST_GeometryFromWKT('POLYGON((1 9, 6 6, 6 4, 1 2,1 9))'),  ST_GeometryFromWKT('POLYGON((2 5, 4 5, 4 1, 2 1, 2 5))'))",
        false);
  }

  @Test
  public void test_ST_Equals() {
    registerUDFV2("ST_Equals", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Equals( ST_GeometryFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'),  ST_GeometryFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Equals( ST_GeometryFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'),  ST_GeometryFromWKT('POLYGON((1 4, 1 2, 4.5 2, 4.5 4, 1 4))'))",
        true);
  }

  @Test
  public void test_ST_Intersects() {
    registerUDFV2("ST_Intersects", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Intersects( ST_GeometryFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'),  ST_GeometryFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))'))",
        false);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Intersects( ST_GeometryFromWKT('POLYGON((1 9, 6 6, 6 4, 1 2,1 9))'),  ST_GeometryFromWKT('POLYGON((2 5, 4 5, 4 1, 2 1, 2 5))'))",
        true);
  }

  @Test
  public void test_ST_OrderingEquals() {
    registerUDFV2("ST_OrderingEquals", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_OrderingEquals( ST_GeometryFromWKT('LINESTRING (0 0, 1 0)'),  ST_GeometryFromWKT('LINESTRING (1 0, 0 0)'))",
        false);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_OrderingEquals( ST_GeometryFromWKT('LINESTRING (0 0, 1 0)'),  ST_GeometryFromWKT('LINESTRING (0 0, 1 0)'))",
        true);
  }

  @Test
  public void test_ST_Overlaps() {
    registerUDFV2("ST_Overlaps", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Overlaps( ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'),  ST_GeometryFromWKT('POLYGON ((0.5 1, 1.5 0, 2 0, 0.5 1))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Overlaps( ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'),  ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'))",
        false);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Overlaps( ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'),  ST_GeometryFromWKT('LINESTRING (0 0, 1 1)'))",
        false);
  }

  @Test
  public void test_ST_Touches() {
    registerUDFV2("ST_Touches", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Touches( ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'),  ST_GeometryFromWKT('POLYGON ((1 1, 1 0, 2 0, 1 1))'))",
        true);
  }

  @Test
  public void test_ST_Relate() {
    registerUDFV2("ST_Relate", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Relate( ST_GeometryFromWKT('LINESTRING (1 1, 5 5)'),  ST_GeometryFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))'))",
        "1010F0212");

    registerUDFV2("ST_Relate", String.class, String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Relate( ST_GeometryFromWKT('LINESTRING (1 1, 5 5)'),  ST_GeometryFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))'), '1010F0212')",
        true);
  }

  @Test
  public void test_ST_RelateMatch() {
    registerUDFV2("ST_RelateMatch", String.class, String.class);
    verifySqlSingleRes("SELECT SEDONA.ST_RelateMatch('101202FFF', 'TTTTTTFFF')", true);
  }

  @Test
  public void test_ST_Within() {
    registerUDFV2("ST_Within", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Within( ST_GeometryFromWKT('POINT (0.5 0.25)'),  ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'))",
        true);
  }

  @Test
  public void test_ST_Covers() {
    registerUDFV2("ST_Covers", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Covers( ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'),  ST_GeometryFromWKT('POINT (1.0 0.0)'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Covers( ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'),  ST_GeometryFromWKT('POINT (0.0 1.0)'))",
        false);
  }

  @Test
  public void test_ST_CoveredBy() {
    registerUDFV2("ST_CoveredBy", String.class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CoveredBy( ST_GeometryFromWKT('POINT (1.0 0.0)'),  ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CoveredBy( ST_GeometryFromWKT('POINT (0.0 1.0)'),  ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'))",
        false);
  }

  @Test
  public void test_ST_DWithin() {
    registerUDFV2("ST_DWithin", String.class, String.class, double.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_DWithin(ST_GeometryFromWKT('POINT (1.5 0.0)'), ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), 0.5)",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_DWithin(ST_GeometryFromWKT('POINT (0.0 1.0)'), ST_GeometryFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), 0.0)",
        false);
  }
}
