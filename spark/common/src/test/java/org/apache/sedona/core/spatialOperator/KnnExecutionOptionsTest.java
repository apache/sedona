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
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class KnnExecutionOptionsTest extends TestBase {
  @BeforeClass
  public static void setup() {
    initialize(KnnExecutionOptionsTest.class.getName());
  }

  @AfterClass
  public static void teardown() {
    sc.stop();
  }

  @Test
  public void acceptsSupportedPlanAndMetricCombinations() {
    JoinQuery.validateKnnExecutionOptions(planarParams(), false, true, true);
    JoinQuery.validateKnnExecutionOptions(planarParams(), true, false, true);

    JoinParams geographyParams =
        new JoinParams(true, null, IndexType.RTREE, null, 1, DistanceMetric.HAVERSINE);
    JoinQuery.validateKnnExecutionOptions(geographyParams, false, false, false);
    JoinQuery.validateKnnExecutionOptions(geographyParams, true, false, false);
  }

  @Test
  public void replicatedReconciliationRejectsBroadcastPlan() {
    assertThrows(
        IllegalArgumentException.class,
        () -> JoinQuery.validateKnnExecutionOptions(planarParams(), true, true, true));
  }

  @Test
  public void replicatedReconciliationRequiresQueryLocalSemantics() {
    assertThrows(
        IllegalArgumentException.class,
        () -> JoinQuery.validateKnnExecutionOptions(planarParams(), false, true, false));
  }

  @Test
  public void queryLocalSemanticsRejectNonPlanarMetric() {
    JoinParams geographyParams =
        new JoinParams(true, null, IndexType.RTREE, null, 1, DistanceMetric.HAVERSINE);
    assertThrows(
        IllegalArgumentException.class,
        () -> JoinQuery.validateKnnExecutionOptions(geographyParams, false, false, true));
  }

  @Test
  public void convenienceOverloadDoesNotRequireInternalRowMetadata() throws Exception {
    GeometryFactory factory = new GeometryFactory();
    SpatialRDD<Point> queries = new SpatialRDD<>();
    queries.setRawSpatialRDD(
        sc.parallelize(Collections.singletonList(factory.createPoint(new Coordinate(0.0, 0.0)))));
    SpatialRDD<Point> objects = new SpatialRDD<>();
    objects.setRawSpatialRDD(
        sc.parallelize(
            Arrays.asList(
                factory.createPoint(new Coordinate(-1.0, 0.0)),
                factory.createPoint(new Coordinate(1.0, 0.0)))));

    JavaPairRDD<Point, Point> matches =
        JoinQuery.knnJoin(queries, objects, planarParams(), false, false, true);

    assertEquals(1L, matches.count());
  }

  private JoinParams planarParams() {
    return new JoinParams(true, null, IndexType.RTREE, null, 1, DistanceMetric.EUCLIDEAN);
  }
}
