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
package org.apache.spark.sql.sedona_sql.types

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.sedona.common.{Constructors, Functions}
import org.apache.sedona.common.S2Geography.{Geography, GeographyWKBSerializer}
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.aggregate.ScalaAggregator
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, GenericInternalRow, Literal, SpecializedGetters, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, GeographyUDT}
import org.apache.spark.sql.sedona_sql.expressions.SerdeAware
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.BinaryView
import org.locationtech.jts.geom.Geometry

/**
 * Exposes native Spark spatial types while keeping Sedona's computational expressions independent
 * of Spark's physical representation. UDT values are confined to the implementation boundary and
 * the reader for existing UDT-backed data. Adjacent Sedona calls cancel the conversion pair.
 */
object SpatialTypeSupport {
  val usesNativeTypes: Boolean = true

  /** The representation adapter does not change the geometry used for spatial pushdown. */
  def unwrapGeometryInput(expression: Expression): Expression = expression match {
    case LegacySpatialInput(child) => child
    case other => other
  }
  def geometryType: DataType = GeometryType("ANY")
  def geometryType(srid: Int): DataType = GeometryType(srid)
  def isNativeSpatial(dataType: DataType): Boolean =
    dataType.isInstanceOf[GeometryType] || dataType.isInstanceOf[GeographyType]
  def isGeometry(dataType: DataType): Boolean =
    dataType.isInstanceOf[GeometryType] || dataType.isInstanceOf[GeometryUDT]
  def serializeGeometry(value: Geometry, dataType: DataType): Any = dataType match {
    case _: GeometryUDT => if (value == null) null else GeometryUDT.serialize(value)
    case native: GeometryType =>
      if (value != null) native.assertSridAllowedForType(value.getSRID)
      serializeGeometry(value)
    case other => throw new IllegalArgumentException(s"Expected a geometry type, got $other")
  }
  def deserializeGeometry(value: Any, dataType: DataType): Geometry = {
    if (value == null) null
    else
      dataType match {
        case _: GeometryUDT => GeometryUDT.deserialize(value)
        case _ => geometry(value)
      }
  }
  def readGeometry(row: SpecializedGetters, ordinal: Int, dataType: DataType): Geometry =
    if (row.isNullAt(ordinal)) null else deserializeGeometry(row.get(ordinal, dataType), dataType)
  def externalGeometry(value: Geometry): Any =
    if (value == null) null
    else org.apache.spark.sql.types.Geometry.fromWKB(isoWkb(value), value.getSRID)
  def fromExternalGeometry(value: Any): Geometry = value match {
    case null => null
    case geom: Geometry => geom
    case geom: org.apache.spark.sql.types.Geometry =>
      Constructors.geomFromWKB(geom.getBytes, geom.getSrid)
  }

  def legacyType(dataType: DataType): DataType = dataType match {
    case _: GeometryType => GeometryUDT()
    case _: GeographyType => GeographyUDT()
    case ArrayType(element, nullable) => ArrayType(legacyType(element), nullable)
    case MapType(key, value, nullable) => MapType(legacyType(key), legacyType(value), nullable)
    case StructType(fields) =>
      StructType(fields.map(f => f.copy(dataType = legacyType(f.dataType))))
    case other => other
  }

  def nativeType(dataType: DataType): DataType = dataType match {
    case _: GeometryUDT => GeometryType("ANY")
    case _: GeographyUDT => GeographyType("ANY")
    case ArrayType(element, nullable) => ArrayType(nativeType(element), nullable)
    case MapType(key, value, nullable) => MapType(nativeType(key), nativeType(value), nullable)
    case StructType(fields) =>
      StructType(fields.map(f => f.copy(dataType = nativeType(f.dataType))))
    case other => other
  }

  /**
   * Sedona's dimension-preserving writer uses EWKB Z/M flag bits even without an SRID. Spark
   * accepts ISO WKB type offsets instead. Rewrite only those headers, preserving coordinate
   * bytes, empty dimensions, and each collection member's dimensionality. This consumes only
   * bytes emitted by Sedona, never arbitrary user input.
   */
  private def isoWkb(geometry: Geometry): Array[Byte] = {
    val bytes = Functions.asWKB(geometry)
    val buffer = ByteBuffer.wrap(bytes)
    def skipCoordinates(count: Int, dimensions: Int): Unit =
      buffer.position(buffer.position() + Math.multiplyExact(count, dimensions * 8))
    def rewriteGeometry(): Unit = {
      buffer.order(if (buffer.get() == 1) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN)
      val typePosition = buffer.position()
      val code = buffer.getInt()
      val hasZ = (code & 0x80000000) != 0
      val hasM = (code & 0x40000000) != 0
      val base = code & 0x1fffffff
      val dimensions = 2 + (if (hasZ) 1 else 0) + (if (hasM) 1 else 0)
      buffer.putInt(typePosition, base + (if (hasZ) 1000 else 0) + (if (hasM) 2000 else 0))
      base match {
        case 1 => skipCoordinates(1, dimensions)
        case 2 => skipCoordinates(buffer.getInt(), dimensions)
        case 3 =>
          val rings = buffer.getInt()
          (0 until rings).foreach(_ => skipCoordinates(buffer.getInt(), dimensions))
        case 4 | 5 | 6 | 7 =>
          val count = buffer.getInt()
          (0 until count).foreach(_ => rewriteGeometry())
        case other => throw new IllegalArgumentException(s"Unsupported WKB geometry type $other")
      }
    }
    rewriteGeometry()
    bytes
  }

  def geometry(value: Any): Geometry = {
    if (value == null) return null
    NativeSpatialReader.geometry(value.asInstanceOf[BinaryView])
  }

  def geography(value: Any): Geography = {
    if (value == null) return null
    NativeSpatialReader.geography(value.asInstanceOf[BinaryView])
  }

  def serializeGeometry(value: Geometry): Any = {
    if (value == null) return null
    GeometryType(value.getSRID) // Validate the native type's SRID contract.
    NativeSpatialWriter.serialize(value, value.getSRID, geography = false)
  }

  def serializeGeography(value: Geography): Any = {
    if (value == null) return null
    GeographyType(value.getSRID)
    val geom = org.apache.sedona.common.geography.Constructors.geogToGeometry(value)
    NativeSpatialWriter.serialize(geom, value.getSRID, geography = true)
  }

  private def convertContainers(
      value: Any,
      dataType: DataType,
      convert: (Any, DataType) => Any): Any = dataType match {
    case ArrayType(element, _) =>
      val array = value.asInstanceOf[ArrayData]
      new GenericArrayData((0 until array.numElements()).map { i =>
        if (array.isNullAt(i)) null else convert(array.get(i, element), element)
      }.toArray)
    case MapType(key, item, _) =>
      val map = value.asInstanceOf[MapData]
      new ArrayBasedMapData(
        convertContainers(map.keyArray(), ArrayType(key, false), convert).asInstanceOf[ArrayData],
        convertContainers(map.valueArray(), ArrayType(item), convert).asInstanceOf[ArrayData])
    case StructType(fields) =>
      val row = value.asInstanceOf[InternalRow]
      new GenericInternalRow(fields.indices.map { i =>
        if (row.isNullAt(i)) null else convert(row.get(i, fields(i).dataType), fields(i).dataType)
      }.toArray)
    case _ => value
  }

  def toLegacyValue(value: Any, dataType: DataType): Any = {
    if (value == null) return null
    dataType match {
      case _: GeometryType => GeometrySerializer.serialize(geometry(value))
      case _: GeographyType => GeographyWKBSerializer.serialize(geography(value))
      case _ => convertContainers(value, dataType, toLegacyValue)
    }
  }

  def toNativeValue(value: Any, dataType: DataType): Any = {
    if (value == null) return null
    dataType match {
      case _: GeometryUDT =>
        serializeGeometry(GeometrySerializer.deserialize(value.asInstanceOf[Array[Byte]]))
      case _: GeographyUDT =>
        serializeGeography(GeographyWKBSerializer.deserialize(value.asInstanceOf[Array[Byte]]))
      case _ => convertContainers(value, dataType, toNativeValue)
    }
  }

  private def legacyInput(expression: Expression): Expression = expression match {
    case NativeSpatialResult(child, _) => child
    case _ if legacyType(expression.dataType) == expression.dataType => expression
    case _
        if expression.dataType.isInstanceOf[GeometryType] ||
          expression.dataType.isInstanceOf[GeographyType] =>
      LegacySpatialInput(expression)
    case _ => LegacySpatialContainer(expression)
  }

  private def resultType(expression: Expression, original: Seq[Expression]): DataType = {
    val default = nativeType(expression.dataType)
    val name = expression.getClass.getSimpleName
    // Function builders run before implicit casts. Only infer from an integer literal;
    // dynamic or not-yet-coerced SRID expressions conservatively retain the ANY type.
    def literalSrid(index: Int, makeType: Int => DataType): DataType =
      expression.children.lift(index) match {
        case Some(Literal(value: Int, IntegerType)) => makeType(value)
        case _ => default
      }
    if (expression.dataType.isInstanceOf[GeometryUDT]) {
      name match {
        case "ST_Point" | "ST_MakePoint" | "ST_MakePointM" | "ST_PolygonFromEnvelope" |
            "ST_GeomFromBox2D" =>
          GeometryType(0)
        case "ST_PointZ" | "ST_PointM" => literalSrid(3, GeometryType.apply)
        case "ST_PointZM" => literalSrid(4, GeometryType.apply)
        case "ST_MakeEnvelope" =>
          if (expression.children.size == 4) GeometryType(0)
          else literalSrid(4, GeometryType.apply)
        case "ST_GeomFromWKT" | "ST_GeomFromText" | "ST_GeometryFromText" =>
          literalSrid(1, GeometryType.apply)
        case "ST_Buffer" | "ST_Reverse" | "ST_Normalize" | "ST_Force2D" =>
          original.headOption
            .map(_.dataType)
            .filter(_.isInstanceOf[GeometryType])
            .getOrElse(default)
        case _ => default
      }
    } else if (expression.dataType.isInstanceOf[GeographyUDT] &&
      Set("ST_GeogFromWKT", "ST_GeogFromText", "ST_GeogCollFromText").contains(name)) {
      literalSrid(1, GeographyType.apply)
    } else default
  }

  def adaptFunction(builder: FunctionBuilder): FunctionBuilder = { children =>
    val inputs = children.map(legacyInput)
    val initial = builder(inputs)
    // Sedona's legacy geography defaults to SRID 0, which is not a native geography CRS.
    val expression =
      if (children.size == 1 &&
        Set("ST_GeogFromWKT", "ST_GeogFromText", "ST_GeogCollFromText").contains(
          initial.getClass.getSimpleName)) {
        builder(inputs :+ Literal(4326))
      } else initial
    val bound = expression match {
      case aggregate: ScalaAggregator[_, _, _] =>
        aggregate.copy(
          inputEncoder = aggregate.inputEncoder.resolveAndBind(),
          bufferEncoder = aggregate.bufferEncoder.resolveAndBind())
      case other => other
    }
    val output = resultType(bound, children)
    if (output == bound.dataType) bound
    else
      bound match {
        case generator: Generator => NativeSpatialGenerator(generator)
        case aggregate: TypedImperativeAggregate[_] =>
          NativeSpatialAggregate(aggregate.asInstanceOf[TypedImperativeAggregate[Any]], output)
        case _ => NativeSpatialResult(bound, output)
      }
  }
}

private[sql] case class LegacySpatialInput(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with SerdeAware {
  override def prettyName: String = child.prettyName
  override def sql: String = child.sql
  override def dataType: DataType = SpatialTypeSupport.legacyType(child.dataType)
  override def nullable: Boolean = child.nullable
  override def foldable: Boolean = child.foldable
  override def evalWithoutSerialization(input: InternalRow): Any = child.dataType match {
    case _: GeometryType => SpatialTypeSupport.geometry(child.eval(input))
    case _: GeographyType => SpatialTypeSupport.geography(child.eval(input))
  }
  override protected def nullSafeEval(value: Any): Any =
    SpatialTypeSupport.toLegacyValue(value, child.dataType)
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

private[sql] case class LegacySpatialContainer(child: Expression)
    extends UnaryExpression
    with CodegenFallback {
  override def prettyName: String = child.prettyName
  override def sql: String = child.sql
  override def dataType: DataType = SpatialTypeSupport.legacyType(child.dataType)
  override def nullable: Boolean = child.nullable
  override def foldable: Boolean = child.foldable
  override protected def nullSafeEval(value: Any): Any =
    SpatialTypeSupport.toLegacyValue(value, child.dataType)
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

private[sql] case class NativeSpatialResult(child: Expression, override val dataType: DataType)
    extends UnaryExpression
    with CodegenFallback {
  override def prettyName: String = child.prettyName
  override def sql: String = child.sql
  override def nullable: Boolean = child.nullable
  override def foldable: Boolean = child.foldable
  override def eval(input: InternalRow): Any = child match {
    case aware: SerdeAware if child.dataType.isInstanceOf[GeometryUDT] =>
      SpatialTypeSupport.serializeGeometry(
        aware.evalWithoutSerialization(input).asInstanceOf[Geometry],
        dataType)
    case aware: SerdeAware if child.dataType.isInstanceOf[GeographyUDT] =>
      SpatialTypeSupport.serializeGeography(
        aware.evalWithoutSerialization(input).asInstanceOf[Geography])
    case _ => SpatialTypeSupport.toNativeValue(child.eval(input), child.dataType)
  }
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

/** Keeps aggregate identity visible to Catalyst, including DISTINCT, FILTER and window calls. */
private[sql] case class NativeSpatialAggregate(
    delegate: TypedImperativeAggregate[Any],
    override val dataType: DataType,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[Any] {
  override def prettyName: String = delegate.prettyName
  override def sql: String = delegate.sql
  override def children: Seq[Expression] = delegate.children
  override def nullable: Boolean = delegate.nullable
  override def checkInputDataTypes() = delegate.checkInputDataTypes()
  override def createAggregationBuffer(): Any = delegate.createAggregationBuffer()
  override def update(buffer: Any, input: InternalRow): Any = delegate.update(buffer, input)
  override def merge(buffer: Any, input: Any): Any = delegate.merge(buffer, input)
  override def eval(buffer: Any): Any =
    SpatialTypeSupport.toNativeValue(delegate.eval(buffer), delegate.dataType)
  override def serialize(buffer: Any): Array[Byte] = delegate.serialize(buffer)
  override def deserialize(bytes: Array[Byte]): Any = delegate.deserialize(bytes)
  override def withNewMutableAggBufferOffset(offset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = offset)
  override def withNewInputAggBufferOffset(offset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = offset)
  override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): Expression =
    copy(delegate =
      delegate.withNewChildren(children).asInstanceOf[TypedImperativeAggregate[Any]])
}

/** Preserves Catalyst's row-generating contract while converting every emitted spatial value. */
private[sql] case class NativeSpatialGenerator(delegate: Generator)
    extends Generator
    with CodegenFallback {
  override def prettyName: String = delegate.prettyName
  override def sql: String = delegate.sql
  override def children: Seq[Expression] = delegate.children
  override def elementSchema: StructType =
    SpatialTypeSupport.nativeType(delegate.elementSchema).asInstanceOf[StructType]
  override def checkInputDataTypes() = delegate.checkInputDataTypes()
  private def convert(rows: IterableOnce[InternalRow]): IterableOnce[InternalRow] =
    rows.iterator.map(row =>
      SpatialTypeSupport.toNativeValue(row, delegate.elementSchema).asInstanceOf[InternalRow])
  override def eval(input: InternalRow): IterableOnce[InternalRow] = convert(delegate.eval(input))
  override def terminate(): IterableOnce[InternalRow] = convert(delegate.terminate())
  override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): Expression =
    copy(delegate = delegate.withNewChildren(children).asInstanceOf[Generator])
}
