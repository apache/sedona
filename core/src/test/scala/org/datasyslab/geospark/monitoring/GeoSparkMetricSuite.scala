/*
 * FILE: GeoSparkMetricSuite.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geospark.monitoring

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, FunSuiteLike}

class GeoSparkMetricSuite extends FunSuite with FunSuiteLike with BeforeAndAfterAll {

  implicit lazy val sc = {
    val conf = new SparkConf().setAppName(classOf[GeoSparkMetricSuite].getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    sc
  }

  override def afterAll(): Unit = {
    sc.stop
  }

  test("simple count") {

    val data = 1 to 100
    val rdd = sc.parallelize(data, 5)

    val metric = GeoSparkMetrics.createMetric(sc, "count")

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
