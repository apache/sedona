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

import org.apache.sedona.common.S2Geography.WKBGeography
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, IsNull, LeafExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.sedona_sql.UDT.{GeographyUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions.{SerdeAware, ST_Distance, ST_Intersects, ST_NPoints}
import org.apache.spark.sql.sedona_sql.expressions.geography.{ST_GeogFromEWKT, ST_GeogToGeometry}
import org.apache.spark.sql.sedona_sql.strategy.join.TraitJoinQueryBase
import org.apache.spark.sql.types.{ArrayType, DataType, GeographyType, GeometryType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory
import org.scalatest.funsuite.AnyFunSuite

/** A nested Sedona call cannot erase the native validation of an intermediate spatial value. */
class NativeNestedValidationSuite extends AnyFunSuite {
  private val empty = InternalRow.empty
  private def point(srid: Int): Geometry =
    new GeometryFactory(new PrecisionModel(), srid).createPoint(new Coordinate(1, 2))
  private def ewkt: Expression = SpatialTypeSupport.adaptFunction(ST_GeogFromEWKT.apply)(
    Seq(BoundReference(0, StringType, nullable = true)))
  private def row(text: String): InternalRow =
    new GenericInternalRow(Array[Any](if (text == null) null else UTF8String.fromString(text)))
  private def distance(input: Expression): Expression =
    SpatialTypeSupport.adaptFunction(ST_Distance.apply)(Seq(input, input))
  private def numberOfPoints(input: Expression): Expression =
    SpatialTypeSupport.adaptFunction(ST_NPoints.apply)(Seq(input))
  private def rejectCrs(body: => Any): Unit = {
    val error = intercept[Exception](body)
    val messages = Iterator
      .iterate[Throwable](error)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage))
      .mkString(" ")
      .toLowerCase
    assert(messages.contains("srid") || messages.contains("crs"), messages)
  }

  for (srid <- Seq(0, 3857)) {
    test(s"invalid geography SRID $srid is rejected standalone and in nested scalar calls") {
      val input = ewkt
      val value = row(s"SRID=$srid;POINT (1 2)")
      rejectCrs(input.eval(value))
      rejectCrs(distance(input).eval(value))
      val converted = SpatialTypeSupport.adaptFunction(ST_GeogToGeometry.apply)(Seq(input))
      rejectCrs(converted.eval(value))
    }
    test(s"invalid geography SRID $srid is rejected by the direct join decoder") {
      val operand = distance(ewkt).children.head
      rejectCrs(TraitJoinQueryBase.shapeToGeography(operand, row(s"SRID=$srid;POINT (1 2)")))
    }
  }

  test("valid geographic CRS and nulls retain scalar and direct join behavior") {
    val input = ewkt
    val value = row("SRID=4269;POINT (1 2)")
    assert(distance(input).eval(value) == 0d)
    val operand = distance(input).children.head
    assert(TraitJoinQueryBase.shapeToGeography(operand, value).getSRID == 4269)
    assert(input.eval(row(null)) == null)
    assert(distance(input).eval(row(null)) == null)
    assert(TraitJoinQueryBase.shapeToGeography(operand, row(null)) == null)
  }

  test("nested geography retains fixed target CRS validation and avoids legacy serialization") {
    val source = RawSpatial(WKBGeography.fromJTS(point(4269)), GeographyUDT())
    val input = NativeSpatialResult(source, GeographyType(4326))
    rejectCrs(input.eval(empty))
    rejectCrs(distance(input).eval(empty))
  }

  test("nested geometry retains fixed target CRS validation and avoids legacy serialization") {
    val input = NativeSpatialResult(RawSpatial(point(3857), GeometryUDT()), GeometryType(4326))
    rejectCrs(input.eval(empty))
    rejectCrs(numberOfPoints(input).eval(empty))
    val predicate = SpatialTypeSupport.adaptFunction(ST_Intersects.apply)(Seq(input, input))
    rejectCrs(TraitJoinQueryBase.shapeToGeometry(predicate.children.head, empty))
  }

  test("negative geometry SRID cannot be hidden by a scalar consumer") {
    val input = NativeSpatialResult(RawSpatial(point(-1), GeometryUDT()), GeometryType("ANY"))
    rejectCrs(input.eval(empty))
    rejectCrs(numberOfPoints(input).eval(empty))
  }

  test("declared mixed multipart layouts are rejected before scalar and join consumption") {
    val sequences = PackedCoordinateSequenceFactory.DOUBLE_FACTORY
    val factory = new GeometryFactory(new PrecisionModel(), 0, sequences)
    val mixed = factory.createMultiPoint(
      Array(
        factory.createPoint(sequences.create(Array(1d, 2d), 2, 0)),
        factory.createPoint(sequences.create(Array(3d, 4d, 5d, 6d), 4, 1))))
    val input = NativeSpatialResult(RawSpatial(mixed, GeometryUDT()), GeometryType("ANY"))
    def rejected(body: => Any): Unit = {
      val error = intercept[Exception](body)
      val messages = Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .flatMap(e => Option(e.getMessage))
        .mkString(" ")
      assert(messages.contains("heterogeneous"), messages)
    }
    rejected(input.eval(empty))
    rejected(numberOfPoints(input).eval(empty))
    val predicate = SpatialTypeSupport.adaptFunction(ST_Intersects.apply)(Seq(input, input))
    rejected(TraitJoinQueryBase.shapeToGeometry(predicate.children.head, empty))
  }

  test("array struct and map boundaries retain their non-computational conversion path") {
    val bytes =
      SpatialTypeSupport.serializeGeometry(point(4326), GeometryUDT()).asInstanceOf[Array[Byte]]
    val arrayType = ArrayType(GeometryUDT(), containsNull = true)
    val array = new GenericArrayData(Array[Any](bytes, null))
    val mapType = MapType(StringType, GeometryUDT(), valueContainsNull = true)
    val map = new ArrayBasedMapData(
      new GenericArrayData(
        Array[Any](UTF8String.fromString("shape"), UTF8String.fromString("absent"))),
      array)
    val structType =
      StructType(Seq(StructField("shape", GeometryUDT()), StructField("absent", GeometryUDT())))
    val struct = new GenericInternalRow(Array[Any](bytes, null))
    Seq[(DataType, Any)](arrayType -> array, mapType -> map, structType -> struct).foreach {
      case (legacyType, value) =>
        Seq(value, null).foreach { source =>
          val native = NativeSpatialResult(
            InternalValue(source, legacyType),
            SpatialTypeSupport.nativeType(legacyType))
          val consumer =
            SpatialTypeSupport.adaptFunction(children => IsNull(children.head))(Seq(native))
          assert(consumer.children.head.isInstanceOf[LegacySpatialContainer])
          assert(!consumer.children.head.isInstanceOf[SerdeAware])
          assert(consumer.eval(empty) == (source == null))
        }
    }
  }

  private case class InternalValue(value: Any, override val dataType: DataType)
      extends LeafExpression
      with CodegenFallback {
    override def nullable: Boolean = value == null
    override def eval(input: InternalRow): Any = value
  }

  private case class RawSpatial(value: Any, override val dataType: DataType)
      extends LeafExpression
      with CodegenFallback
      with SerdeAware {
    override def nullable: Boolean = value == null
    override def eval(input: InternalRow): Any =
      throw new AssertionError("The computational path must not evaluate legacy SQL bytes")
    override def evalWithoutSerialization(input: InternalRow): Any = value
  }
}
