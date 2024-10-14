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

import java.util.Arrays;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

public class JoinQueryDeduplicationTest extends TestBase {

  @BeforeClass
  public static void setup() {
    initialize(JoinQueryDeduplicationTest.class.getName());
  }

  @AfterClass
  public static void teardown() {
    sc.stop();
  }

  /** See https://issues.apache.org/jira/browse/SEDONA-233 */
  @Test
  public void testDeduplication() throws Exception {
    SpatialRDD<Geometry> leftRDD = new SpatialRDD<>();
    leftRDD.setRawSpatialRDD(
        sc.parallelize(
                Arrays.asList(
                    "POLYGON ((3 0, 3 3, 0 3, 0 0, 3 0))",
                    "POLYGON ((4 1, 4 4, 1 4, 1 1, 4 1))",
                    "POLYGON ((3 1, 3 4, 0 4, 0 1, 3 1))",
                    "POLYGON ((4 0, 4 3, 1 3, 1 0, 4 0))"))
            .map(wkt -> Constructors.geomFromWKT(wkt, 0)));
    leftRDD.analyze();
    leftRDD.spatialPartitioning(GridType.KDBTREE, 2);

    SpatialRDD<Geometry> rightRDD = new SpatialRDD<>();
    rightRDD.setRawSpatialRDD(
        sc.parallelize(Arrays.asList("POLYGON ((4 0, 4 4, 0 4, 0 0, 4 0))"))
            .map(wkt -> Constructors.geomFromWKT(wkt, 0)));
    rightRDD.spatialPartitioning(leftRDD.getPartitioner());

    JavaPairRDD<Geometry, Geometry> joined =
        JoinQuery.spatialJoin(
            leftRDD, rightRDD, new JoinQuery.JoinParams(false, SpatialPredicate.INTERSECTS));
    assertEquals(8, joined.union(joined).count());
  }
}
