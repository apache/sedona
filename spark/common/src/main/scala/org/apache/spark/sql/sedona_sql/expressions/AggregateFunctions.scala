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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.sedona.common.Functions
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import org.locationtech.jts.operation.overlayng.OverlayNGRobust

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * traits for creating Aggregate Function
 */

trait TraitSTAggregateExec {
  val initialGeometry: Geometry = null
  val serde = ExpressionEncoder[Geometry]()

  def zero: Geometry = initialGeometry

  def bufferEncoder: ExpressionEncoder[Geometry] = serde

  def outputEncoder: ExpressionEncoder[Geometry] = serde

  def finish(out: Geometry): Geometry = out
}

private[apache] class ST_Union_Aggr(bufferSize: Int = 1000)
    extends Aggregator[Geometry, ListBuffer[Geometry], Geometry] {

  val serde = ExpressionEncoder[Geometry]()
  val bufferSerde = ExpressionEncoder[ListBuffer[Geometry]]()

  override def reduce(buffer: ListBuffer[Geometry], input: Geometry): ListBuffer[Geometry] = {
    if (input != null) {
      buffer += input
    }
    if (buffer.size >= bufferSize) {
      // Perform the union when buffer size is reached
      val unionGeometry = OverlayNGRobust.union(buffer.asJava)
      buffer.clear()
      buffer += unionGeometry
    }
    buffer
  }

  override def merge(
      buffer1: ListBuffer[Geometry],
      buffer2: ListBuffer[Geometry]): ListBuffer[Geometry] = {
    buffer1 ++= buffer2
    if (buffer1.size >= bufferSize) {
      // Perform the union when buffer size is reached
      val unionGeometry = OverlayNGRobust.union(buffer1.asJava)
      buffer1.clear()
      buffer1 += unionGeometry
    }
    buffer1
  }

  override def finish(reduction: ListBuffer[Geometry]): Geometry = {
    if (reduction.isEmpty) {
      return null
    }
    OverlayNGRobust.union(reduction.asJava)
  }

  def bufferEncoder: ExpressionEncoder[ListBuffer[Geometry]] = bufferSerde

  def outputEncoder: ExpressionEncoder[Geometry] = serde

  override def zero: ListBuffer[Geometry] = ListBuffer.empty
}

/**
 * A helper class to store envelope boundary during aggregation. We use this custom case class
 * instead of JTS Envelope to work with the Spark Encoder.
 */
case class EnvelopeBuffer(minX: Double, maxX: Double, minY: Double, maxY: Double) {
  def isNull: Boolean = minX > maxX

  def toEnvelope: Envelope = {
    if (isNull) {
      new Envelope()
    } else {
      new Envelope(minX, maxX, minY, maxY)
    }
  }

  def merge(other: EnvelopeBuffer): EnvelopeBuffer = {
    if (this.isNull) {
      other
    } else if (other.isNull) {
      this
    } else {
      EnvelopeBuffer(
        math.min(this.minX, other.minX),
        math.max(this.maxX, other.maxX),
        math.min(this.minY, other.minY),
        math.max(this.maxY, other.maxY))
    }
  }
}

/**
 * Return the envelope boundary of the entire column
 */
private[apache] class ST_Envelope_Aggr
    extends Aggregator[Geometry, Option[EnvelopeBuffer], Geometry] {

  val serde = ExpressionEncoder[Geometry]()

  def reduce(buffer: Option[EnvelopeBuffer], input: Geometry): Option[EnvelopeBuffer] = {
    if (input == null) return buffer
    val env = input.getEnvelopeInternal
    val envBuffer = EnvelopeBuffer(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    buffer match {
      case Some(b) => Some(b.merge(envBuffer))
      case None => Some(envBuffer)
    }
  }

  def merge(
      buffer1: Option[EnvelopeBuffer],
      buffer2: Option[EnvelopeBuffer]): Option[EnvelopeBuffer] = {
    (buffer1, buffer2) match {
      case (Some(b1), Some(b2)) => Some(b1.merge(b2))
      case (Some(_), None) => buffer1
      case (None, Some(_)) => buffer2
      case (None, None) => None
    }
  }

  def finish(reduction: Option[EnvelopeBuffer]): Geometry = {
    reduction match {
      case Some(b) => new GeometryFactory().toGeometry(b.toEnvelope)
      case None => null
    }
  }

  def bufferEncoder: Encoder[Option[EnvelopeBuffer]] = Encoders.product[Option[EnvelopeBuffer]]

  def outputEncoder: ExpressionEncoder[Geometry] = serde

  def zero: Option[EnvelopeBuffer] = None
}

/**
 * Return the polygon intersection of all Polygon in the given column
 */
private[apache] class ST_Intersection_Aggr
    extends Aggregator[Geometry, Geometry, Geometry]
    with TraitSTAggregateExec {
  def reduce(buffer: Geometry, input: Geometry): Geometry = {
    if (input == null) {
      return buffer
    }
    if (buffer == null) {
      return input
    }
    buffer.intersection(input)
  }

  def merge(buffer1: Geometry, buffer2: Geometry): Geometry = {
    if (buffer1 == null) {
      return buffer2
    }
    if (buffer2 == null) {
      return buffer1
    }
    buffer1.intersection(buffer2)
  }

  override def finish(out: Geometry): Geometry = out
}

/**
 * Return a multi-geometry collection of all geometries in the given column. Unlike ST_Union_Aggr,
 * this function does not dissolve boundaries between geometries.
 */
private[apache] class ST_Collect_Agg
    extends Aggregator[Geometry, ListBuffer[Geometry], Geometry] {

  val serde = ExpressionEncoder[Geometry]()
  val bufferSerde = ExpressionEncoder[ListBuffer[Geometry]]()

  override def reduce(buffer: ListBuffer[Geometry], input: Geometry): ListBuffer[Geometry] = {
    if (input != null) {
      buffer += input
    }
    buffer
  }

  override def merge(
      buffer1: ListBuffer[Geometry],
      buffer2: ListBuffer[Geometry]): ListBuffer[Geometry] = {
    buffer1 ++= buffer2
    buffer1
  }

  override def finish(reduction: ListBuffer[Geometry]): Geometry = {
    if (reduction.isEmpty) {
      null
    } else {
      Functions.createMultiGeometry(reduction.toArray)
    }
  }

  def bufferEncoder: ExpressionEncoder[ListBuffer[Geometry]] = bufferSerde

  def outputEncoder: ExpressionEncoder[Geometry] = serde

  override def zero: ListBuffer[Geometry] = ListBuffer.empty
}
