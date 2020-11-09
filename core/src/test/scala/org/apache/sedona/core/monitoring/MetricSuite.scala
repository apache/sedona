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

package org.apache.sedona.core.monitoring

import org.apache.sedona.core.SparkUtil
import org.apache.spark.TaskContext
import org.scalatest.FunSuiteLike

class MetricSuite extends SparkUtil with FunSuiteLike {

  test("simple count") {

    val data = 1 to 100
    val rdd = sc.parallelize(data, 5)

    val metric = Metrics.createMetric(sc, "count")

    // Count elements in each partition. For partitions with even IDs count each element twice.
    rdd.map { x =>
      if (TaskContext.getPartitionId() % 2 == 0)
        metric.add(2)
      else
        metric.add(1)
      x
    }.collect

    // Verify metric
    val expectedPartitionIds = 0 to 4
    assert(expectedPartitionIds == metric.value.keys.toSeq.sorted)

    for ((partitionId, count) <- metric.value) {
      val expectedCount = if (partitionId % 2 == 0) 40 else 20
      assert(count == expectedCount)
    }
  }
}
