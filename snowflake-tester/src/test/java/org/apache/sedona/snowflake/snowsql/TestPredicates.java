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
public class TestPredicates extends TestBase {

  public void test_ST_Contains() {
    registerUDF("ST_Contains", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CONTAINS(sedona.ST_GeomFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), sedona.ST_GeomFromWKT('POINT(0.5 0.5)'))",
        true);
  }

  @Test
  public void test_ST_Crosses() {
    registerUDF("ST_Crosses", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Crosses(sedona.ST_GeomFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), sedona.ST_GeomFromWKT('LINESTRING(0.5 0.5, 1.1 0.5)'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Crosses(sedona.ST_GeomFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), sedona.ST_GeomFromWKT('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
        false);
  }

  @Test
  public void test_ST_Disjoint() {
    registerUDF("ST_Disjoint", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Disjoint(SEDONA.ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'), SEDONA.ST_GeomFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Disjoint(SEDONA.ST_GeomFromWKT('POLYGON((1 9, 6 6, 6 4, 1 2,1 9))'), SEDONA.ST_GeomFromWKT('POLYGON((2 5, 4 5, 4 1, 2 1, 2 5))'))",
        false);
  }

  @Test
  public void test_ST_Equals() {
    registerUDF("ST_Equals", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Equals(SEDONA.ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'), SEDONA.ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Equals(SEDONA.ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'), SEDONA.ST_GeomFromWKT('POLYGON((1 4, 1 2, 4.5 2, 4.5 4, 1 4))'))",
        true);
  }

  @Test
  public void test_ST_Intersects() {
    registerUDF("ST_Intersects", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Intersects(SEDONA.ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'), SEDONA.ST_GeomFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))'))",
        false);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Intersects(SEDONA.ST_GeomFromWKT('POLYGON((1 9, 6 6, 6 4, 1 2,1 9))'), SEDONA.ST_GeomFromWKT('POLYGON((2 5, 4 5, 4 1, 2 1, 2 5))'))",
        true);
  }

  @Test
  public void test_ST_OrderingEquals() {
    registerUDF("ST_OrderingEquals", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_OrderingEquals(SEDONA.ST_GeomFromWKT('LINESTRING (0 0, 1 0)'), SEDONA.ST_GeomFromWKT('LINESTRING (1 0, 0 0)'))",
        false);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_OrderingEquals(SEDONA.ST_GeomFromWKT('LINESTRING (0 0, 1 0)'), SEDONA.ST_GeomFromWKT('LINESTRING (0 0, 1 0)'))",
        true);
  }

  @Test
  public void test_ST_Overlaps() {
    registerUDF("ST_Overlaps", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Overlaps(SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), SEDONA.ST_GeomFromWKT('POLYGON ((0.5 1, 1.5 0, 2 0, 0.5 1))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Overlaps(SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'))",
        false);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Overlaps(SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), SEDONA.ST_GeomFromWKT('LINESTRING (0 0, 1 1)'))",
        false);
  }

  @Test
  public void test_ST_Touches() {
    registerUDF("ST_Touches", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Touches(SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), SEDONA.ST_GeomFromWKT('POLYGON ((1 1, 1 0, 2 0, 1 1))'))",
        true);
  }

  @Test
  public void test_ST_Relate() {
    registerUDF("ST_Relate", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Relate(SEDONA.ST_GeomFromWKT('LINESTRING (1 1, 5 5)'), SEDONA.ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))'))",
        "1010F0212");
    registerUDF("ST_Relate", byte[].class, byte[].class, String.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Relate(SEDONA.ST_GeomFromWKT('LINESTRING (1 1, 5 5)'), SEDONA.ST_GeomFromWKT('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))'), '1010F0212')",
        true);
  }

  @Test
  public void test_ST_RelateMatch() {
    registerUDF("ST_RelateMatch", String.class, String.class);
    verifySqlSingleRes("SELECT SEDONA.ST_RelateMatch('101202FFF', 'TTTTTTFFF')", true);
  }

  @Test
  public void test_ST_Within() {
    registerUDF("ST_Within", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Within(SEDONA.ST_GeomFromWKT('POINT (0.5 0.25)'), SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'))",
        true);
  }

  @Test
  public void test_ST_Covers() {
    registerUDF("ST_Covers", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Covers(SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), SEDONA.ST_GeomFromWKT('POINT (1.0 0.0)'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_Covers(SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), SEDONA.ST_GeomFromWKT('POINT (0.0 1.0)'))",
        false);
  }

  @Test
  public void test_ST_CoveredBy() {
    registerUDF("ST_CoveredBy", byte[].class, byte[].class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CoveredBy(SEDONA.ST_GeomFromWKT('POINT (1.0 0.0)'), SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'))",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_CoveredBy(SEDONA.ST_GeomFromWKT('POINT (0.0 1.0)'), SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'))",
        false);
  }

  @Test
  public void test_ST_DWithin() {
    registerUDF("ST_DWithin", byte[].class, byte[].class, double.class);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_DWithin(SEDONA.ST_GeomFromWKT('POINT (1.5 0.0)'), SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), 0.5)",
        true);
    verifySqlSingleRes(
        "SELECT SEDONA.ST_DWithin(SEDONA.ST_GeomFromWKT('POINT (0.0 1.0)'), SEDONA.ST_GeomFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 0))'), 0.0)",
        false);
  }
}
