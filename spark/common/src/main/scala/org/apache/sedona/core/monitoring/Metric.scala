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

import org.apache.spark.TaskContext
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * An accumulator to collect custom per-task metrics into a map
  * keyed by partition ID processed by the task.
  */
case class Metric(initialValue: Map[Int, Long] = Map()) extends AccumulatorV2[Long, Map[Int, Long]] {
  private var _counts: mutable.Map[Int, Long] = mutable.Map[Int, Long]() ++ initialValue

  override def isZero: Boolean = _counts.isEmpty

  override def copy(): AccumulatorV2[Long, Map[Int, Long]] = new Metric(_counts.toMap)

  override def reset(): Unit = _counts.clear()

  override def add(v: Long): Unit = add(TaskContext.getPartitionId, v)

  private def add(partitionId: Int, value: Long) = {
    _counts(partitionId) = value + _counts.getOrElse(partitionId, 0L)
  }

  override def merge(other: AccumulatorV2[Long, Map[Int, Long]]): Unit = {
    other.asInstanceOf[Metric]._counts.foreach {
      case (partitionId, value) => add(partitionId, value)
    }
  }

  override def value: Map[Int, Long] = _counts.toMap
}
