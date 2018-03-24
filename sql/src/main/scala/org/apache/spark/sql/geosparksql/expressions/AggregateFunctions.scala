/*
 * FILE: AggregateFunctions.scala
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
package org.apache.spark.sql.geosparksql.expressions

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Polygon}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
  * Return the polygon union of all Polygon in the given column
  */

class ST_Union_Aggr extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("Union", new GeometryUDT) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("Union", new GeometryUDT) :: Nil
  )

  override def dataType: DataType = new GeometryUDT

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(-999999999, -999999999)
    coordinates(1) = new Coordinate(-999999999, -999999999)
    coordinates(2) = new Coordinate(-999999999, -999999999)
    coordinates(3) = new Coordinate(-999999999, -999999999)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    buffer(0) = geometryFactory.createPolygon(coordinates)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val accumulateUnion = buffer.getAs[Polygon](0)
    val newPolygon = input.getAs[Polygon](0)
    if (accumulateUnion.getArea == 0) buffer(0) = newPolygon
    else buffer(0) = accumulateUnion.union(newPolygon)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftPolygon = buffer1.getAs[Polygon](0)
    val rightPolygon = buffer2.getAs[Polygon](0)
    if (leftPolygon.getCoordinates()(0).x == -999999999) buffer1(0) = rightPolygon
    else if (rightPolygon.getCoordinates()(0).x == -999999999) buffer1(0) = leftPolygon
    else buffer1(0) = leftPolygon.union(rightPolygon)
  }

  override def evaluate(buffer: Row): Any = {
    return buffer.getAs[Geometry](0)
  }
}

/**
  * Return the envelope boundary of the entire column
  */
class ST_Envelope_Aggr extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("Envelope", new GeometryUDT) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("Envelope", new GeometryUDT) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = new GeometryUDT

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(-999999999, -999999999)
    coordinates(1) = new Coordinate(-999999999, -999999999)
    coordinates(2) = new Coordinate(-999999999, -999999999)
    coordinates(3) = new Coordinate(-999999999, -999999999)
    coordinates(4) = new Coordinate(-999999999, -999999999)
    val geometryFactory = new GeometryFactory()
    buffer(0) = geometryFactory.createPolygon(coordinates)
    //buffer(0) = new GenericArrayData(GeometrySerializer.serialize(geometryFactory.createPolygon(coordinates)))
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val accumulateEnvelope = buffer.getAs[Geometry](0).getEnvelopeInternal
    val newEnvelope = input.getAs[Geometry](0).getEnvelopeInternal
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    var minX = 0.0
    var minY = 0.0
    var maxX = 0.0
    var maxY = 0.0
    if (accumulateEnvelope.getMinX == -999999999) {
      // Found the accumulateEnvelope is the initial value
      minX = newEnvelope.getMinX
      minY = newEnvelope.getMinY
      maxX = newEnvelope.getMaxX
      maxY = newEnvelope.getMaxY
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
    buffer(0) = geometryFactory.createPolygon(coordinates)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftEnvelope = buffer1.getAs[Geometry](0).getEnvelopeInternal
    val rightEnvelope = buffer2.getAs[Geometry](0).getEnvelopeInternal
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    var minX = 0.0
    var minY = 0.0
    var maxX = 0.0
    var maxY = 0.0
    if (leftEnvelope.getMinX == -999999999) {
      // Found the leftEnvelope is the initial value
      minX = rightEnvelope.getMinX
      minY = rightEnvelope.getMinY
      maxX = rightEnvelope.getMaxX
      maxY = rightEnvelope.getMaxY
    }
    else if (rightEnvelope.getMinX == -999999999) {
      // Found the rightEnvelope is the initial value
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
    buffer1(0) = geometryFactory.createPolygon(coordinates)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    return buffer.getAs[Geometry](0)
  }
}
