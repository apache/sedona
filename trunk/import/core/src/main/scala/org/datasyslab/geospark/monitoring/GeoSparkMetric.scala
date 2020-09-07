/*
 * FILE: GeoSparkMetric.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.datasyslab.geospark.monitoring

import org.apache.spark.TaskContext
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * An accumulator to collect custom per-task metrics into a map
  * keyed by partition ID processed by the task.
  */
case class GeoSparkMetric(initialValue: Map[Int, Long] = Map()) extends AccumulatorV2[Long, Map[Int, Long]] {
  private var _counts: mutable.Map[Int, Long] = mutable.Map[Int, Long]() ++ initialValue

  override def isZero: Boolean = _counts.isEmpty

  override def copy(): AccumulatorV2[Long, Map[Int, Long]] = new GeoSparkMetric(_counts.toMap)

  override def reset(): Unit = _counts.clear()

  override def add(v: Long): Unit = add(TaskContext.getPartitionId, v)

  private def add(partitionId: Int, value: Long) = {
    _counts(partitionId) = value + _counts.getOrElse(partitionId, 0L)
  }

  override def merge(other: AccumulatorV2[Long, Map[Int, Long]]): Unit = {
    other.asInstanceOf[GeoSparkMetric]._counts.foreach {
      case (partitionId, value) => add(partitionId, value)
    }
  }

  override def value: Map[Int, Long] = _counts.toMap
}
