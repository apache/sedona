/*
 * FILE: GeoSparkMetric.scala
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
