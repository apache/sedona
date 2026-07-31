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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.sedona.common.Functions
import org.apache.sedona.common.S2Geography.GeographyWKBSerializer
import org.apache.sedona.common.geography.{Functions => GeographyFunctions}
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.sedona_sql.UDT.{GeographyUDT, GeometryUDT}
import org.apache.spark.sql.types.{AbstractDataType, DataType, TypeCollection}

/**
 * Collect all non-null spatial values in a column without dissolving boundaries.
 *
 * Unlike a typed [[org.apache.spark.sql.expressions.Aggregator]], this Catalyst aggregate can
 * preserve the logical type of its child: Geometry inputs produce Geometry and Geography inputs
 * produce Geography. The aggregation buffer retains the already-serialized UDT values so it can
 * be shuffled without choosing one UDT encoder for both overloads.
 */
private[apache] case class ST_Collect_Agg(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[ArrayBuffer[Array[Byte]]]
    with ImplicitCastInputTypes
    with UnaryLike[Expression] {

  def this(inputExpressions: Seq[Expression]) =
    this(ST_Collect_Agg.requireSingleInput(inputExpressions), 0, 0)

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType match {
    case _: GeographyUDT => GeographyUDT()
    case _ => GeometryUDT()
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(GeometryUDT(), GeographyUDT()))

  private def isGeography: Boolean = child.dataType.isInstanceOf[GeographyUDT]

  override def createAggregationBuffer(): ArrayBuffer[Array[Byte]] = ArrayBuffer.empty

  override def update(
      buffer: ArrayBuffer[Array[Byte]],
      input: InternalRow): ArrayBuffer[Array[Byte]] = {
    val value = child.eval(input).asInstanceOf[Array[Byte]]
    if (value != null) {
      if (isGeography && buffer.nonEmpty) {
        ST_Collect_Agg.requireMatchingSRID(buffer.head, value)
      }
      // Catalyst can reuse input-row storage, so retain an owned copy in the aggregation buffer.
      buffer += value.clone()
    }
    buffer
  }

  override def merge(
      buffer: ArrayBuffer[Array[Byte]],
      other: ArrayBuffer[Array[Byte]]): ArrayBuffer[Array[Byte]] = {
    if (isGeography && buffer.nonEmpty && other.nonEmpty) {
      ST_Collect_Agg.requireMatchingSRID(buffer.head, other.head)
    }
    buffer ++= other
  }

  override def eval(buffer: ArrayBuffer[Array[Byte]]): Any = {
    if (buffer.isEmpty) {
      null
    } else if (isGeography) {
      val geographies = buffer.map(GeographyWKBSerializer.deserialize)
      GeographyWKBSerializer.serialize(
        GeographyFunctions.createMultiGeography(geographies.toArray))
    } else {
      val geometries = buffer.map(GeometrySerializer.deserialize)
      GeometrySerializer.serialize(Functions.createMultiGeometry(geometries.toArray))
    }
  }

  override def serialize(buffer: ArrayBuffer[Array[Byte]]): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(bytes)
    output.writeInt(buffer.size)
    buffer.foreach { value =>
      output.writeInt(value.length)
      output.write(value)
    }
    output.flush()
    bytes.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): ArrayBuffer[Array[Byte]] = {
    if (storageFormat == null) {
      return createAggregationBuffer()
    }

    val input = new DataInputStream(new ByteArrayInputStream(storageFormat))
    val count = input.readInt()
    if (count < 0) {
      throw new IllegalArgumentException(s"Invalid ST_Collect_Agg buffer count: $count")
    }

    val buffer = new ArrayBuffer[Array[Byte]](count)
    var index = 0
    while (index < count) {
      val length = input.readInt()
      if (length < 0 || length > input.available()) {
        throw new IllegalArgumentException(s"Invalid ST_Collect_Agg buffer item length: $length")
      }
      val value = new Array[Byte](length)
      input.readFully(value)
      buffer += value
      index += 1
    }
    buffer
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): ST_Collect_Agg =
    copy(child = newChild)

  override def prettyName: String = "st_collect_agg"
}

private object ST_Collect_Agg {

  private def requireSingleInput(inputExpressions: Seq[Expression]): Expression = {
    if (inputExpressions.length != 1) {
      throw new IllegalArgumentException(
        s"ST_Collect_Agg requires exactly one argument, but got ${inputExpressions.length}")
    }
    inputExpressions.head
  }

  private def readGeographySRID(value: Array[Byte]): Int = {
    if (value.length < Integer.BYTES) {
      throw new IllegalArgumentException("Invalid serialized Geography in ST_Collect_Agg")
    }
    ((value(0) & 0xff) << 24) |
      ((value(1) & 0xff) << 16) |
      ((value(2) & 0xff) << 8) |
      (value(3) & 0xff)
  }

  private def requireMatchingSRID(left: Array[Byte], right: Array[Byte]): Unit = {
    val leftSRID = readGeographySRID(left)
    val rightSRID = readGeographySRID(right)
    if (leftSRID != rightSRID) {
      throw new IllegalArgumentException(
        s"ST_Collect_Agg requires all Geography inputs to have the same SRID; " +
          s"found $leftSRID and $rightSRID")
    }
  }
}
