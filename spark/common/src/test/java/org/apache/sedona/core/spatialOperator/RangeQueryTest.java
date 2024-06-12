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
package org.apache.sedona.core.spatialOperator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

@RunWith(Parameterized.class)
public class RangeQueryTest extends SpatialQueryTestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() throws IOException {
    initialize(RangeQueryTest.class.getSimpleName(), "spatial-predicates-test-data.tsv");
  }

  @AfterClass
  public static void teardown() {
    sc.stop();
  }

  @Parameterized.Parameters(name = "RangeQueryTest-{index}: {0}")
  public static SpatialPredicate[] spatialPredicates() {
    return SpatialPredicate.values();
  }

  private final SpatialPredicate spatialPredicate;
  private final Geometry queryWindow;
  private final List<Integer> expectedResults;

  public RangeQueryTest(SpatialPredicate predicate) throws ParseException {
    this.spatialPredicate = predicate;
    queryWindow = queryWindowOf(predicate);
    expectedResults = buildExpectedResults(spatialPredicate, queryWindow);
    Assert.assertFalse("expected results should not be empty", expectedResults.isEmpty());
  }

  private static List<Integer> buildExpectedResults(
      SpatialPredicate spatialPredicate, Geometry queryWindow) {
    return testDataset.entrySet().stream()
        .filter(entry -> evaluateSpatialPredicate(spatialPredicate, entry.getValue(), queryWindow))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  private static Geometry queryWindowOf(SpatialPredicate spatialPredicate) throws ParseException {
    WKTReader wktReader = new WKTReader();
    switch (spatialPredicate) {
      case INTERSECTS:
      case WITHIN:
      case COVERED_BY:
      case CROSSES:
      case OVERLAPS:
      case TOUCHES:
        return wktReader.read("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))");
      case CONTAINS:
      case COVERS:
        return wktReader.read("POINT (10 10)");
      case EQUALS:
        return wktReader.read("POLYGON ((0 10, 1 10, 1 11, 0 11, 0 10))");
      default:
        throw new IllegalArgumentException("Unsupported spatial predicate: " + spatialPredicate);
    }
  }

  @Test
  public void testRangeQueryRaw() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    JavaRDD<Geometry> queryResultRdd =
        RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, spatialPredicate, false);
    verifyQueryResult(queryResultRdd);
  }

  @Test
  public void testRangeQueryWithIndex() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.buildIndex(IndexType.RTREE, false);
    JavaRDD<Geometry> queryResultRdd =
        RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, spatialPredicate, true);
    verifyQueryResult(queryResultRdd);
  }

  @Test
  public void testRangeQueryWithConsiderBoundaryIntersection() throws Exception {
    if (spatialPredicate == SpatialPredicate.INTERSECTS
        || spatialPredicate == SpatialPredicate.COVERED_BY) {
      boolean considerBoundaryIntersection = spatialPredicate == SpatialPredicate.INTERSECTS;
      SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
      spatialRDD.rawSpatialRDD = inputRdd;
      JavaRDD<Geometry> queryResultRdd =
          RangeQuery.SpatialRangeQuery(
              spatialRDD, queryWindow, considerBoundaryIntersection, false);
      verifyQueryResult(queryResultRdd);
    }
  }

  private void verifyQueryResult(JavaRDD<Geometry> resultRdd) {
    List<Integer> actualResults =
        resultRdd.map(geom -> ((UserData) geom.getUserData()).getId()).collect();
    Assert.assertEquals(
        "Number of results should match with expected results",
        expectedResults.size(),
        actualResults.size());
    List<Integer> sortedActualResults =
        actualResults.stream().sorted(Integer::compareTo).collect(Collectors.toList());
    List<Integer> sortedExpectedResults =
        expectedResults.stream().sorted(Integer::compareTo).collect(Collectors.toList());
    Assert.assertEquals(
        "Actual range query result should match with expected results",
        sortedExpectedResults,
        sortedActualResults);
  }
}
