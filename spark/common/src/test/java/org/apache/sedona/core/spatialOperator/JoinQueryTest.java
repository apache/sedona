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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialRDD.CircleRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Geometry;

@RunWith(Parameterized.class)
public class JoinQueryTest extends SpatialQueryTestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() throws IOException {
    initialize(JoinQueryTest.class.getSimpleName(), "spatial-predicates-test-data.tsv");
  }

  @AfterClass
  public static void teardown() {
    sc.stop();
  }

  @Parameterized.Parameters(name = "JoinQueryTest-{index}: {0}")
  public static SpatialPredicate[] spatialPredicates() {
    return SpatialPredicate.values();
  }

  private final SpatialPredicate spatialPredicate;
  private final JavaRDD<Geometry> queryDataRdd;
  private final Map<Integer, Geometry> queryWindowDataset;
  private final Map<Integer, List<Integer>> expectedResults;

  public JoinQueryTest(SpatialPredicate predicate) throws IOException {
    this.spatialPredicate = predicate;
    String queryWindowPath = "spatial-join-query-window.tsv";
    this.queryDataRdd = readTestDataAsRDD(queryWindowPath);
    this.queryWindowDataset = readTestDataAsMap(queryWindowPath);
    expectedResults = buildExpectedResults(spatialPredicate);
    Assert.assertFalse("expected results should not be empty", expectedResults.isEmpty());
  }

  private Map<Integer, List<Integer>> buildExpectedResults(SpatialPredicate spatialPredicate) {
    Map<Integer, List<Integer>> results = new HashMap<>();
    queryWindowDataset.forEach(
        (queryWindowId, queryWindow) ->
            testDataset.forEach(
                (geomId, value) -> {
                  if (evaluateSpatialPredicate(spatialPredicate, value, queryWindow)) {
                    results.computeIfAbsent(queryWindowId, k -> new ArrayList<>()).add(geomId);
                  }
                }));
    results.forEach((k, v) -> v.sort(Integer::compareTo));
    return results;
  }

  private Map<Integer, List<Integer>> spatialJoinResultRddToIdMap(
      JavaPairRDD<Geometry, List<Geometry>> resultRdd) {
    Map<Integer, List<Integer>> resultIdMap = new HashMap<>();
    resultRdd
        .collect()
        .forEach(
            pair -> {
              Geometry queryWindow = pair._1;
              List<Geometry> queryResults = pair._2;
              int queryWindowId = ((UserData) queryWindow.getUserData()).getId();
              List<Integer> geomIds =
                  queryResults.stream()
                      .map(geom -> ((UserData) geom.getUserData()).getId())
                      .sorted(Integer::compareTo)
                      .collect(Collectors.toList());
              resultIdMap.put(queryWindowId, geomIds);
            });
    return resultIdMap;
  }

  private Map<Integer, List<Integer>> spatialJoinFlatResultRddToIdMap(
      JavaPairRDD<Geometry, Geometry> resultRdd) {
    Map<Integer, List<Integer>> resultIdMap = new HashMap<>();
    resultRdd
        .collect()
        .forEach(
            pair -> {
              Geometry queryWindow = pair._1;
              Geometry queryResult = pair._2;
              int queryWindowId = ((UserData) queryWindow.getUserData()).getId();
              int geomId = ((UserData) queryResult.getUserData()).getId();
              resultIdMap.computeIfAbsent(queryWindowId, k -> new ArrayList<>()).add(geomId);
            });
    resultIdMap.forEach((k, v) -> v.sort(Integer::compareTo));
    return resultIdMap;
  }

  @Test
  public void testSpatialJoinWithoutIndex() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
    JavaPairRDD<Geometry, List<Geometry>> actualResultRdd =
        JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, spatialPredicate);
    verifySpatialJoinResult(actualResultRdd);
  }

  @Test
  public void testSpatialJoinWithSpatialRddIndex() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    spatialRDD.buildIndex(IndexType.RTREE, true);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
    JavaPairRDD<Geometry, List<Geometry>> actualResultRdd =
        JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, true, spatialPredicate);
    verifySpatialJoinResult(actualResultRdd);
  }

  @Test
  public void testSpatialJoinWithQueryWindowIndex() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
    queryRDD.buildIndex(IndexType.RTREE, true);
    JavaPairRDD<Geometry, List<Geometry>> actualResultRdd =
        JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, true, spatialPredicate);
    verifySpatialJoinResult(actualResultRdd);
  }

  @Test
  public void testSpatialJoinWithDynamicIndex() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
    JavaPairRDD<Geometry, List<Geometry>> actualResultRdd =
        JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, true, spatialPredicate);
    verifySpatialJoinResult(actualResultRdd);
  }

  @Test
  public void testSpatialJoinFlat() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
    JavaPairRDD<Geometry, Geometry> actualResultRdd =
        JoinQuery.SpatialJoinQueryFlat(spatialRDD, queryRDD, false, spatialPredicate);
    verifySpatialJoinFlatResult(actualResultRdd);
  }

  @Test
  public void testSpatialJoinCountByKey() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
    JavaPairRDD<Geometry, Long> actualResultRdd =
        JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, queryRDD, false, spatialPredicate);
    verifySpatialJoinCountByKeyResult(actualResultRdd);
  }

  @Test
  public void testDistanceJoin() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    CircleRDD circleRDD = new CircleRDD(queryRDD, 1.0);
    circleRDD.spatialPartitioning(spatialRDD.getPartitioner());
    if (spatialPredicate == SpatialPredicate.INTERSECTS
        || spatialPredicate == SpatialPredicate.COVERED_BY) {
      boolean considerBoundaryIntersection = spatialPredicate == SpatialPredicate.INTERSECTS;
      JavaPairRDD<Geometry, List<Geometry>> actualResultRdd =
          JoinQuery.DistanceJoinQuery(spatialRDD, circleRDD, false, spatialPredicate);
      JavaPairRDD<Geometry, List<Geometry>> expectedResultRdd =
          JoinQuery.DistanceJoinQuery(spatialRDD, circleRDD, false, considerBoundaryIntersection);
      verifySpatialJoinResult(expectedResultRdd, actualResultRdd);
    } else {
      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> JoinQuery.DistanceJoinQuery(spatialRDD, circleRDD, false, spatialPredicate));
    }
  }

  @Test
  public void testDistanceJoinFlat() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    CircleRDD circleRDD = new CircleRDD(queryRDD, 1.0);
    circleRDD.spatialPartitioning(spatialRDD.getPartitioner());
    if (spatialPredicate == SpatialPredicate.INTERSECTS
        || spatialPredicate == SpatialPredicate.COVERED_BY) {
      boolean considerBoundaryIntersection = spatialPredicate == SpatialPredicate.INTERSECTS;
      JavaPairRDD<Geometry, Geometry> actualResultRdd =
          JoinQuery.DistanceJoinQueryFlat(spatialRDD, circleRDD, false, spatialPredicate);
      JavaPairRDD<Geometry, Geometry> expectedResultRdd =
          JoinQuery.DistanceJoinQueryFlat(
              spatialRDD, circleRDD, false, considerBoundaryIntersection);
      verifySpatialJoinFlatResult(expectedResultRdd, actualResultRdd);
    } else {
      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> JoinQuery.DistanceJoinQueryFlat(spatialRDD, circleRDD, false, spatialPredicate));
    }
  }

  @Test
  public void testDistanceJoinCountByKey() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    CircleRDD circleRDD = new CircleRDD(queryRDD, 1.0);
    circleRDD.spatialPartitioning(spatialRDD.getPartitioner());
    if (spatialPredicate == SpatialPredicate.INTERSECTS
        || spatialPredicate == SpatialPredicate.COVERED_BY) {
      boolean considerBoundaryIntersection = spatialPredicate == SpatialPredicate.INTERSECTS;
      JavaPairRDD<Geometry, Long> actualResultRdd =
          JoinQuery.DistanceJoinQueryCountByKey(spatialRDD, circleRDD, false, spatialPredicate);
      JavaPairRDD<Geometry, Long> expectedResultRdd =
          JoinQuery.DistanceJoinQueryCountByKey(
              spatialRDD, circleRDD, false, considerBoundaryIntersection);
      verifySpatialJoinCountByKeyResult(expectedResultRdd, actualResultRdd);
    } else {
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              JoinQuery.DistanceJoinQueryCountByKey(
                  spatialRDD, circleRDD, false, spatialPredicate));
    }
  }

  @Test
  public void testSpatialJoinWithConsiderBoundaryIntersection() throws Exception {
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.rawSpatialRDD = inputRdd;
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 10);
    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.rawSpatialRDD = queryDataRdd;
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
    if (spatialPredicate == SpatialPredicate.INTERSECTS
        || spatialPredicate == SpatialPredicate.COVERED_BY) {
      boolean considerBoundaryIntersection = spatialPredicate == SpatialPredicate.INTERSECTS;
      {
        JavaPairRDD<Geometry, List<Geometry>> actualResultRdd =
            JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, considerBoundaryIntersection);
        verifySpatialJoinResult(actualResultRdd);
      }
      {
        JavaPairRDD<Geometry, Geometry> actualResultRdd =
            JoinQuery.SpatialJoinQueryFlat(
                spatialRDD, queryRDD, false, considerBoundaryIntersection);
        verifySpatialJoinFlatResult(actualResultRdd);
      }
      {
        JavaPairRDD<Geometry, Long> actualResultRdd =
            JoinQuery.SpatialJoinQueryCountByKey(
                spatialRDD, queryRDD, false, considerBoundaryIntersection);
        verifySpatialJoinCountByKeyResult(actualResultRdd);
      }
    }
  }

  private void verifySpatialJoinResult(JavaPairRDD<Geometry, List<Geometry>> actualResultRdd) {
    Map<Integer, List<Integer>> actualResults = spatialJoinResultRddToIdMap(actualResultRdd);
    Assert.assertEquals(
        "Actual result of spatial join should match expected results",
        expectedResults,
        actualResults);
  }

  private void verifySpatialJoinFlatResult(JavaPairRDD<Geometry, Geometry> actualResultRdd) {
    Map<Integer, List<Integer>> actualResults = spatialJoinFlatResultRddToIdMap(actualResultRdd);
    Assert.assertEquals(
        "Actual result of spatial join should match expected results",
        expectedResults,
        actualResults);
  }

  private void verifySpatialJoinCountByKeyResult(JavaPairRDD<Geometry, Long> actualResultRdd) {
    Map<Integer, Long> actualResults = new HashMap<>();
    actualResultRdd
        .collect()
        .forEach(
            pair -> {
              Geometry queryWindow = pair._1;
              Long count = pair._2;
              int queryWindowId = ((UserData) queryWindow.getUserData()).getId();
              actualResults.put(queryWindowId, count);
            });
    Map<Integer, Long> expectedResultsCountByKey =
        expectedResults.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (long) e.getValue().size()));
    Assert.assertEquals(
        "Actual result of spatial join should match expected results",
        expectedResultsCountByKey,
        actualResults);
  }

  private void verifySpatialJoinResult(
      JavaPairRDD<Geometry, List<Geometry>> expectedResultRdd,
      JavaPairRDD<Geometry, List<Geometry>> actualResultRdd) {
    Map<Integer, List<Integer>> expected = spatialJoinResultRddToIdMap(expectedResultRdd);
    Map<Integer, List<Integer>> actual = spatialJoinResultRddToIdMap(actualResultRdd);
    Assert.assertEquals(
        "Actual result of distance join should match expected results", expected, actual);
  }

  private void verifySpatialJoinFlatResult(
      JavaPairRDD<Geometry, Geometry> expectedResultRdd,
      JavaPairRDD<Geometry, Geometry> actualResultRdd) {
    Map<Integer, List<Integer>> expected = spatialJoinFlatResultRddToIdMap(expectedResultRdd);
    Map<Integer, List<Integer>> actual = spatialJoinFlatResultRddToIdMap(actualResultRdd);
    Assert.assertEquals(
        "Actual result of distance join should match expected results", expected, actual);
  }

  private void verifySpatialJoinCountByKeyResult(
      JavaPairRDD<Geometry, Long> expectedResultRdd, JavaPairRDD<Geometry, Long> actualResultRdd) {
    Assert.assertEquals(
        "Actual result of distance join should match expected results",
        expectedResultRdd.collectAsMap(),
        actualResultRdd.collectAsMap());
  }
}
