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
import org.apache.sedona.common.geometryObjects.{Box2D, Box3D}
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
    if (input == null || input.isEmpty) return buffer
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
 * Return the planar bounding box (Box2D) of all geometries in the given column. Returns NULL when
 * the input contains no rows or all rows are null/empty geometries. Mirrors PostGIS ST_Extent.
 *
 * ST_Envelope_Aggr is left untouched (returns a polygon Geometry) for backwards compatibility.
 */
private[apache] class ST_Extent extends Aggregator[Geometry, Option[EnvelopeBuffer], Box2D] {

  val outputSerde: ExpressionEncoder[Box2D] = ExpressionEncoder[Box2D]()

  def reduce(buffer: Option[EnvelopeBuffer], input: Geometry): Option[EnvelopeBuffer] = {
    if (input == null || input.isEmpty) return buffer
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

  def finish(reduction: Option[EnvelopeBuffer]): Box2D = {
    reduction match {
      case Some(b) => new Box2D(b.minX, b.minY, b.maxX, b.maxY)
      case None => null
    }
  }

  def bufferEncoder: Encoder[Option[EnvelopeBuffer]] = Encoders.product[Option[EnvelopeBuffer]]

  def outputEncoder: ExpressionEncoder[Box2D] = outputSerde

  def zero: Option[EnvelopeBuffer] = None
}

/**
 * Aggregator-buffer for the 3D extent. Geometries without a Z dimension fold into the `z = 0`
 * plane on a per-coordinate basis, matching PostGIS's flat-XY-treated-as-XY[Z=0] convention.
 */
case class Envelope3DBuffer(
    minX: Double,
    maxX: Double,
    minY: Double,
    maxY: Double,
    minZ: Double,
    maxZ: Double) {
  def isNull: Boolean = minX > maxX

  def merge(other: Envelope3DBuffer): Envelope3DBuffer = {
    if (this.isNull) other
    else if (other.isNull) this
    else
      Envelope3DBuffer(
        math.min(this.minX, other.minX),
        math.max(this.maxX, other.maxX),
        math.min(this.minY, other.minY),
        math.max(this.maxY, other.maxY),
        math.min(this.minZ, other.minZ),
        math.max(this.maxZ, other.maxZ))
  }
}

/**
 * Return the 3D bounding box (Box3D) of all geometries in the given column. Returns NULL when the
 * input contains no rows or all rows are null/empty geometries. Mirrors PostGIS `ST_3DExtent`.
 * Geometries without a Z dimension are treated as having `z = 0`.
 */
private[apache] class ST_3DExtent extends Aggregator[Geometry, Option[Envelope3DBuffer], Box3D] {

  val outputSerde: ExpressionEncoder[Box3D] = ExpressionEncoder[Box3D]()

  def reduce(buffer: Option[Envelope3DBuffer], input: Geometry): Option[Envelope3DBuffer] = {
    if (input == null || input.isEmpty) return buffer
    val box = Box3D.fromGeometry(input)
    if (box == null) return buffer
    val incoming = Envelope3DBuffer(
      box.getXMin,
      box.getXMax,
      box.getYMin,
      box.getYMax,
      box.getZMin,
      box.getZMax)
    buffer match {
      case Some(b) => Some(b.merge(incoming))
      case None => Some(incoming)
    }
  }

  def merge(
      buffer1: Option[Envelope3DBuffer],
      buffer2: Option[Envelope3DBuffer]): Option[Envelope3DBuffer] = {
    (buffer1, buffer2) match {
      case (Some(b1), Some(b2)) => Some(b1.merge(b2))
      case (Some(_), None) => buffer1
      case (None, Some(_)) => buffer2
      case (None, None) => None
    }
  }

  def finish(reduction: Option[Envelope3DBuffer]): Box3D = {
    reduction match {
      case Some(b) => new Box3D(b.minX, b.minY, b.minZ, b.maxX, b.maxY, b.maxZ)
      case None => null
    }
  }

  def bufferEncoder: Encoder[Option[Envelope3DBuffer]] =
    Encoders.product[Option[Envelope3DBuffer]]

  def outputEncoder: ExpressionEncoder[Box3D] = outputSerde

  def zero: Option[Envelope3DBuffer] = None
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
