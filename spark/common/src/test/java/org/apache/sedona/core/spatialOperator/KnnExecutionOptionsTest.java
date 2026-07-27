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

import static org.junit.Assert.assertThrows;

import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams;
import org.junit.Test;

public class KnnExecutionOptionsTest {
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

  private JoinParams planarParams() {
    return new JoinParams(true, null, IndexType.RTREE, null, 1, DistanceMetric.EUCLIDEAN);
  }
}
