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

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

/**
  * traits for creating Aggregate Function
  */

trait TraitSTAggregateExec {
  val initialGeometry: Geometry = {
    // dummy value for initial value(polygon but )
    // any other value is ok.
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(-999999999, -999999999)
    coordinates(1) = new Coordinate(-999999999, -999999999)
    coordinates(2) = new Coordinate(-999999999, -999999999)
    coordinates(3) = new Coordinate(-999999999, -999999999)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    geometryFactory.createPolygon(coordinates)
  }
  val serde = ExpressionEncoder[Geometry]()

  def zero: Geometry = initialGeometry

  def bufferEncoder: ExpressionEncoder[Geometry] = serde

  def outputEncoder: ExpressionEncoder[Geometry] = serde

  def finish(out: Geometry): Geometry = out
}

/**
  * Return the polygon union of all Polygon in the given column
  */
class ST_Union_Aggr extends Aggregator[Geometry, Geometry, Geometry] with TraitSTAggregateExec {

  def reduce(buffer: Geometry, input: Geometry): Geometry = {
    if (buffer.equalsExact(initialGeometry)) input
    else buffer.union(input)
  }

  def merge(buffer1: Geometry, buffer2: Geometry): Geometry = {
    if (buffer1.equals(initialGeometry)) buffer2
    else if (buffer2.equals(initialGeometry)) buffer1
    else buffer1.union(buffer2)
  }


}


/**
  * Return the envelope boundary of the entire column
  */
class ST_Envelope_Aggr extends Aggregator[Geometry, Geometry, Geometry] with TraitSTAggregateExec {

  def reduce(buffer: Geometry, input: Geometry): Geometry = {
    val accumulateEnvelope = buffer.getEnvelopeInternal
    val newEnvelope = input.getEnvelopeInternal
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    var minX = 0.0
    var minY = 0.0
    var maxX = 0.0
    var maxY = 0.0
    if (accumulateEnvelope.equals(initialGeometry.getEnvelopeInternal)) {
      // Found the accumulateEnvelope is the initial value
      minX = newEnvelope.getMinX
      minY = newEnvelope.getMinY
      maxX = newEnvelope.getMaxX
      maxY = newEnvelope.getMaxY
    }
    else if (newEnvelope.equals(initialGeometry.getEnvelopeInternal)) {
      minX = accumulateEnvelope.getMinX
      minY = accumulateEnvelope.getMinY
      maxX = accumulateEnvelope.getMaxX
      maxY = accumulateEnvelope.getMaxY
    }
    else {
      minX = Math.min(accumulateEnvelope.getMinX, newEnvelope.getMinX)
      minY = Math.min(accumulateEnvelope.getMinY, newEnvelope.getMinY)
      maxX = Math.max(accumulateEnvelope.getMaxX, newEnvelope.getMaxX)
      maxY = Math.max(accumulateEnvelope.getMaxY, newEnvelope.getMaxY)
    }
    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    geometryFactory.createPolygon(coordinates)

  }

  def merge(buffer1: Geometry, buffer2: Geometry): Geometry = {
    val leftEnvelope = buffer1.getEnvelopeInternal
    val rightEnvelope = buffer2.getEnvelopeInternal
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    var minX = 0.0
    var minY = 0.0
    var maxX = 0.0
    var maxY = 0.0
    if (leftEnvelope.equals(initialGeometry.getEnvelopeInternal)) {
      minX = rightEnvelope.getMinX
      minY = rightEnvelope.getMinY
      maxX = rightEnvelope.getMaxX
      maxY = rightEnvelope.getMaxY
    }
    else if (rightEnvelope.equals(initialGeometry.getEnvelopeInternal)) {
      minX = leftEnvelope.getMinX
      minY = leftEnvelope.getMinY
      maxX = leftEnvelope.getMaxX
      maxY = leftEnvelope.getMaxY
    }
    else {
      minX = Math.min(leftEnvelope.getMinX, rightEnvelope.getMinX)
      minY = Math.min(leftEnvelope.getMinY, rightEnvelope.getMinY)
      maxX = Math.max(leftEnvelope.getMaxX, rightEnvelope.getMaxX)
      maxY = Math.max(leftEnvelope.getMaxY, rightEnvelope.getMaxY)
    }

    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    geometryFactory.createPolygon(coordinates)
  }


}

/**
  * Return the polygon intersection of all Polygon in the given column
  */
class ST_Intersection_Aggr extends Aggregator[Geometry, Geometry, Geometry] with TraitSTAggregateExec {
  def reduce(buffer: Geometry, input: Geometry): Geometry = {
    if (buffer.isEmpty) input
    else if (buffer.equalsExact(initialGeometry)) input
    else buffer.intersection(input)
  }

  def merge(buffer1: Geometry, buffer2: Geometry): Geometry = {
    if (buffer1.equalsExact(initialGeometry)) buffer2
    else if (buffer2.equalsExact(initialGeometry)) buffer1
    else buffer1.intersection(buffer2)
  }
}

