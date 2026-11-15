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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;

public class JoinQueryHashCollisionTest extends TestBase {

  private static final String LOWER_TRIANGLE = "POLYGON ((0 0, 8 0, 0 8, 0 0))";
  private static final String UPPER_TRIANGLE = "POLYGON ((8 8, 0 8, 8 0, 8 8))";

  @BeforeClass
  public static void setup() {
    initialize(JoinQueryHashCollisionTest.class.getName());
  }

  @AfterClass
  public static void teardown() {
    sc.stop();
  }

  @Test
  public void distinctGeometriesWithTheSameHashRemainSeparate() throws Exception {
    Geometry lower = geometry(LOWER_TRIANGLE, null);
    Geometry upper = geometry(UPPER_TRIANGLE, null);
    // JTS hashes these different triangles by their identical envelopes.
    assertEquals(GeomUtils.hashCode(lower), GeomUtils.hashCode(upper));
    assertFalse(lower.equalsExact(upper));

    Map<String, List<String>> expected = new TreeMap<>();
    expected.put(key(lower), Arrays.asList("POINT (1 1)", "POINT (2 2)"));
    expected.put(key(upper), Arrays.asList("POINT (6 6)", "POINT (7 7)"));
    assertEquals(expected, spatialJoin(Arrays.asList(lower, upper)));
  }

  @Test
  public void distinctGeometriesWithEqualUserDataRemainSeparate() throws Exception {
    Geometry lower = geometry(LOWER_TRIANGLE, "query");
    Geometry upper = geometry(UPPER_TRIANGLE, "query");
    assertEquals(GeomUtils.hashCode(lower), GeomUtils.hashCode(upper));

    Map<String, List<String>> expected = new TreeMap<>();
    expected.put(key(lower), Arrays.asList("POINT (1 1)", "POINT (2 2)"));
    expected.put(key(upper), Arrays.asList("POINT (6 6)", "POINT (7 7)"));
    assertEquals(expected, spatialJoin(Arrays.asList(lower, upper)));
  }

  @Test
  public void collidingUserDataOnEqualGeometriesRemainSeparate() throws Exception {
    Geometry first = geometry(LOWER_TRIANGLE, "Aa");
    Geometry second = geometry(LOWER_TRIANGLE, "BB");
    assertEquals("Aa".hashCode(), "BB".hashCode());
    assertEquals(GeomUtils.hashCode(first), GeomUtils.hashCode(second));

    Map<String, List<String>> expected = new TreeMap<>();
    expected.put(key(first), Arrays.asList("POINT (1 1)", "POINT (2 2)"));
    expected.put(key(second), Arrays.asList("POINT (1 1)", "POINT (2 2)"));
    assertEquals(expected, spatialJoin(Arrays.asList(first, second)));
  }

  @Test
  public void equalGeometriesAndUserDataStillAggregateAllMatches() throws Exception {
    Geometry first = geometry(LOWER_TRIANGLE, "same");
    Geometry second = geometry(LOWER_TRIANGLE, "same");

    Map<String, List<String>> expected = new TreeMap<>();
    expected.put(
        key(first), Arrays.asList("POINT (1 1)", "POINT (1 1)", "POINT (2 2)", "POINT (2 2)"));
    assertEquals(expected, spatialJoin(Arrays.asList(first, second)));
  }

  @Test
  public void queriesWithoutMatchesProduceNoGroups() throws Exception {
    Geometry outside = geometry("POLYGON ((20 20, 22 20, 22 22, 20 22, 20 20))", null);
    assertEquals(Collections.emptyMap(), spatialJoin(Collections.singletonList(outside)));
  }

  private static Geometry geometry(String wkt, Object userData) throws Exception {
    Geometry geometry = new WKTReader().read(wkt);
    geometry.setUserData(userData);
    return geometry;
  }

  private static String key(Geometry geometry) {
    return geometry.toText() + "|" + geometry.getUserData();
  }

  private static Map<String, List<String>> spatialJoin(List<Geometry> queries) throws Exception {
    List<Geometry> points =
        Arrays.asList(
            geometry("POINT (1 1)", null),
            geometry("POINT (2 2)", null),
            geometry("POINT (6 6)", null),
            geometry("POINT (7 7)", null));
    SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
    spatialRDD.setRawSpatialRDD(sc.parallelize(points, 2));
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(GridType.KDBTREE, 1);

    SpatialRDD<Geometry> queryRDD = new SpatialRDD<>();
    queryRDD.setRawSpatialRDD(sc.parallelize(queries, 2));
    queryRDD.spatialPartitioning(spatialRDD.getPartitioner());

    List<Tuple2<Geometry, List<Geometry>>> groups =
        JoinQuery.SpatialJoinQuery(
                spatialRDD, queryRDD, new JoinQuery.JoinParams(false, SpatialPredicate.INTERSECTS))
            .collect();
    Map<String, List<String>> actual = new TreeMap<>();
    for (Tuple2<Geometry, List<Geometry>> group : groups) {
      List<String> matches = new ArrayList<>();
      for (Geometry point : group._2) {
        matches.add(point.toText());
      }
      Collections.sort(matches);
      assertNull("Duplicate output group", actual.put(key(group._1), matches));
    }
    return actual;
  }
}
