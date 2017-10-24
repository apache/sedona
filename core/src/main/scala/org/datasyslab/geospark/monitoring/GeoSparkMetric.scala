package org.datasyslab.geospark.monitoring

import org.apache.spark.{SparkEnv, TaskContext}
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
