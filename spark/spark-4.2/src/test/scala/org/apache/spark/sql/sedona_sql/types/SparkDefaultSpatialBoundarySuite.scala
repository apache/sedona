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

import java.nio.ByteOrder
import java.util.Arrays

import org.apache.sedona.common.Functions
import org.apache.sedona.common.S2Geography.{Geography, WKBGeography}
import org.apache.spark.{SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast, GenericInternalRow, LeafExpression, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{Geometry => NativeGeometry, Geography => NativeGeography}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, GeographyUDT}
import org.apache.spark.sql.sedona_sql.expressions.{SerdeAware, ST_Buffer, ST_Reverse}
import org.apache.spark.sql.types.{BooleanType, DataType, GeometryType, GeographyType}
import org.apache.spark.unsafe.types.BinaryView
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory
import org.scalatest.funsuite.AnyFunSuite

/** Pure Catalyst boundary checks; no SparkSession or serializer replacement is required. */
class SparkDefaultSpatialBoundarySuite extends AnyFunSuite {
  test("native geometry uses the public Spark Catalyst codec classes") {
    val catalystSource = classOf[
      org.apache.spark.sql.catalyst.InternalRow].getProtectionDomain.getCodeSource.getLocation
    Seq(
      "org.apache.spark.sql.catalyst.util.Geometry",
      "org.apache.spark.sql.catalyst.util.Geography",
      "org.apache.spark.sql.catalyst.util.geo.WkbReader",
      "org.apache.spark.sql.catalyst.util.geo.WkbWriter").foreach { name =>
      val actual = Class.forName(name).getProtectionDomain.getCodeSource.getLocation
      assert(actual == catalystSource, s"$name loaded from $actual rather than $catalystSource")
    }
  }

  private def point(srid: Int): Geometry =
    new GeometryFactory(new PrecisionModel(), srid).createPoint(new Coordinate(1, 2))

  private def lazyGeography(srid: Int): WKBGeography =
    WKBGeography.fromWKB(Functions.asWKB(point(srid)), srid)

  private def assertUnparsed(value: WKBGeography): Unit = {
    Seq("jtsGeometry", "s2Geography", "shapeIndexGeography").foreach { name =>
      val field = classOf[WKBGeography].getDeclaredField(name)
      field.setAccessible(true)
      assert(field.get(value) == null, name)
    }
  }

  test("geometry uses Spark defaults and preserves factory SRID and fixed target checks") {
    val binary = SpatialTypeSupport.serializeGeometry(point(4326), GeometryType(4326))
    assert(binary.isInstanceOf[BinaryView])
    val spark = NativeGeometry.fromValue(binary.asInstanceOf[BinaryView])
    assert(spark.srid() == 4326)
    val decoded = SpatialTypeSupport.deserializeGeometry(binary, GeometryType(4326))
    assert(decoded.getSRID == 4326)
    assert(decoded.getFactory.getSRID == 4326)
    assert(decoded.getEnvelope.getSRID == 4326)
    intercept[SparkRuntimeException] {
      SpatialTypeSupport.serializeGeometry(point(3857), GeometryType(4326))
    }
    intercept[SparkRuntimeException] {
      SpatialTypeSupport.deserializeGeometry(binary, GeometryType(3857))
    }
    assert(SpatialTypeSupport.geometrySrid(GeometryType(4326)).contains(4326))
    assert(SpatialTypeSupport.geometrySrid(GeometryType("ANY")).isEmpty)
    assert(SpatialTypeSupport.geometrySrid(GeometryUDT()).isEmpty)
  }

  test("XYZM and dimensional empty geometry survive public WKB and default Spark serialization") {
    val sequences = PackedCoordinateSequenceFactory.DOUBLE_FACTORY
    val factory = new GeometryFactory(new PrecisionModel(), 4326, sequences)
    Seq(0, 2).foreach { size =>
      val sequence = sequences.create(size, 4, 1)
      (0 until size).foreach { row =>
        (0 until 4).foreach(ordinate => sequence.setOrdinate(row, ordinate, row + ordinate + 1d))
      }
      val input = factory.createLineString(sequence)
      val bytes = SpatialTypeSupport.serializeGeometry(input, GeometryType(4326))
      val output = SpatialTypeSupport
        .deserializeGeometry(bytes, GeometryType(4326))
        .asInstanceOf[org.locationtech.jts.geom.LineString]
        .getCoordinateSequence
      assert(output.size() == size)
      assert(output.getDimension == 4)
      assert(output.getMeasures == 1)
      (0 until size).foreach { row =>
        (0 until 4).foreach(ordinate =>
          assert(output.getOrdinate(row, ordinate) == sequence.getOrdinate(row, ordinate)))
      }
    }
  }

  test("geography preserves lazy S2 ownership and applies the declared native CRS contract") {
    val source = lazyGeography(4326)
    val before = source.getWKBBytes.clone()
    assertUnparsed(source)
    val view = SpatialTypeSupport
      .serializeGeography(source, GeographyType(4326))
      .asInstanceOf[BinaryView]
    assertUnparsed(source)
    assert(Arrays.equals(source.getWKBBytes, before))
    val rowStorage = view.getBytes.clone()
    val decoded = SpatialTypeSupport
      .deserializeGeography(BinaryView.fromBytes(rowStorage), GeographyType(4326))
      .asInstanceOf[WKBGeography]
    assertUnparsed(decoded)
    val retained = decoded.getWKBBytes.clone()
    Arrays.fill(rowStorage, 0.toByte)
    assert(Arrays.equals(decoded.getWKBBytes, retained))
    assert(decoded.getSRID == 4326)
    assert(NativeGeography.fromValue(view).toWkb(ByteOrder.LITTLE_ENDIAN).nonEmpty)
    intercept[SparkRuntimeException] {
      SpatialTypeSupport.serializeGeography(source, GeographyType(4269))
    }
    intercept[SparkIllegalArgumentException] {
      SpatialTypeSupport.serializeGeography(lazyGeography(0), GeographyType("ANY"))
    }
  }

  test(
    "explicitly heterogeneous multipart and polygon declarations are rejected before writer promotion") {
    val sequences = PackedCoordinateSequenceFactory.DOUBLE_FACTORY
    val factory = new GeometryFactory(new PrecisionModel(), 4326, sequences)
    val multi = factory.createMultiPoint(
      Array(
        factory.createPoint(sequences.create(0, 2, 0)),
        factory.createPoint(sequences.create(0, 4, 1))))
    val polygon = factory.createPolygon(
      factory.createLinearRing(sequences.create(0, 2, 0)),
      Array(factory.createLinearRing(sequences.create(0, 4, 1))))
    Seq(multi, polygon).foreach { input =>
      intercept[IllegalArgumentException] {
        org.apache.sedona.common.geometrySerde.GeometrySerializer.getCoordinateType(input)
      }
      intercept[IllegalArgumentException] {
        SpatialTypeSupport.serializeGeometry(input, GeometryType(4326))
      }
    }
    val mixedCollection = factory.createGeometryCollection(
      Array(
        factory.createPoint(sequences.create(0, 2, 0)),
        factory.createPoint(sequences.create(0, 4, 1))))
    intercept[SparkIllegalArgumentException] {
      SpatialTypeSupport.serializeGeometry(mixedCollection, GeometryType(4326))
    }
  }

  test("explicit UDT boundaries retain their own geography CRS and geometry representations") {
    val legacyGeometry = SpatialTypeSupport.serializeGeometry(point(0), GeometryUDT())
    assert(legacyGeometry.isInstanceOf[Array[Byte]])
    assert(SpatialTypeSupport.deserializeGeometry(legacyGeometry, GeometryUDT()).getSRID == 0)
    val legacyGeography = SpatialTypeSupport.serializeGeography(lazyGeography(0), GeographyUDT())
    assert(legacyGeography.isInstanceOf[Array[Byte]])
    assert(SpatialTypeSupport.deserializeGeography(legacyGeography, GeographyUDT()).getSRID == 0)
    assert(SpatialTypeSupport.serializeGeometry(null, GeometryType(4326)) == null)
    assert(SpatialTypeSupport.deserializeGeometry(null, GeometryUDT()) == null)
    assert(SpatialTypeSupport.serializeGeography(null, GeographyType(4326)) == null)
    assert(SpatialTypeSupport.deserializeGeography(null, GeographyUDT()) == null)
  }

  test("SerdeAware scalar results avoid legacy serialization, including adjacent Sedona calls") {
    val input = SpatialObject(point(4326), GeometryUDT())
    val native = NativeSpatialResult(input, GeometryType(4326))
    val result = native.eval(InternalRow.empty)
    assert(SpatialTypeSupport.deserializeGeometry(result, GeometryType(4326)).getSRID == 4326)
    val nested = SpatialTypeSupport.adaptFunction(ST_Reverse.apply)(Seq(native))
    val retainedBoundary = nested.children.head.children.head
    assert(retainedBoundary.isInstanceOf[LegacySpatialInput])
    assert(retainedBoundary.children.head eq native)
    val reversed = nested.eval(InternalRow.empty)
    assert(SpatialTypeSupport.deserializeGeometry(reversed, GeometryType(4326)).getSRID == 4326)
    val wrongType = NativeSpatialResult(input, GeometryType(3857))
    intercept[SparkRuntimeException](wrongType.eval(InternalRow.empty))
  }

  test("buffer result CRS follows literal spheroidal flags and the planar default") {
    Seq(0, 4326, 3857).foreach { srid =>
      val inputType = GeometryType(srid)
      val source =
        Literal(SpatialTypeSupport.serializeGeometry(point(srid), inputType), inputType)
      Seq(Seq.empty, Seq(Literal(false)), Seq(Literal(true))).foreach { flag =>
        val result =
          SpatialTypeSupport.adaptFunction(ST_Buffer.apply)(Seq(source, Literal(1d)) ++ flag)
        val expectedSrid = if (flag == Seq(Literal(true)) && srid == 0) 4326 else srid
        assert(result.dataType == GeometryType(expectedSrid))
        val geometry =
          SpatialTypeSupport.deserializeGeometry(result.eval(InternalRow.empty), result.dataType)
        assert(geometry.getSRID == expectedSrid)
        assert(geometry.getGeometryType == "Polygon")
        assert(!geometry.isEmpty)
      }
    }
  }

  test("buffer result CRS supports per-row spheroidal flags and nulls") {
    Seq(GeometryType(0), GeometryType(4326), GeometryType("ANY")).foreach { inputType =>
      val result = SpatialTypeSupport.adaptFunction(ST_Buffer.apply)(
        Seq(
          BoundReference(0, inputType, nullable = true),
          Literal(1d),
          BoundReference(1, BooleanType, nullable = true)))
      val expectedType = if (inputType == GeometryType(4326)) inputType else GeometryType("ANY")
      assert(result.dataType == expectedType)
      val srids = if (inputType.isMixedSrid) Seq(0, 4326) else Seq(inputType.srid)
      srids.foreach { srid =>
        val source = SpatialTypeSupport.serializeGeometry(point(srid), inputType)
        Seq[Any](false, true, null).foreach { flag =>
          val value = result.eval(new GenericInternalRow(Array[Any](source, flag)))
          if (flag == null) assert(value == null)
          else {
            val geometry = SpatialTypeSupport.deserializeGeometry(value, result.dataType)
            val expectedSrid = if (flag == true && srid == 0) 4326 else srid
            assert(geometry.getSRID == expectedSrid)
            assert(!geometry.isEmpty)
          }
        }
      }
      assert(result.eval(new GenericInternalRow(Array[Any](null, true))) == null)
    }
  }

  test("buffer result CRS defers foldable Boolean flags and preserves ANSI cast errors") {
    Seq(0, 4326).foreach { srid =>
      val inputType = GeometryType(srid)
      val source =
        Literal(SpatialTypeSupport.serializeGeometry(point(srid), inputType), inputType)
      Seq("false", "true").foreach { flag =>
        val result = SpatialTypeSupport.adaptFunction(ST_Buffer.apply)(
          Seq(source, Literal(1d), Cast(Literal(flag), BooleanType, ansiEnabled = true)))
        assert(result.dataType == (if (srid == 0) GeometryType("ANY") else inputType))
        val geometry =
          SpatialTypeSupport.deserializeGeometry(result.eval(InternalRow.empty), result.dataType)
        assert(geometry.getSRID == (if (flag == "true" && srid == 0) 4326 else srid))
      }
    }
    val source =
      Literal(SpatialTypeSupport.serializeGeometry(point(0), GeometryType(0)), GeometryType(0))
    val invalidFlag = SpatialTypeSupport.adaptFunction(ST_Buffer.apply)(
      Seq(source, Literal(1d), Cast(Literal("invalid"), BooleanType, ansiEnabled = true)))
    assert(invalidFlag.dataType == GeometryType("ANY"))
    val error = intercept[Exception] { invalidFlag.eval(InternalRow.empty) }
    assert(error.getMessage.contains("CAST_INVALID_INPUT"))
  }

  test("native input adapters expose computational objects and unwrap only planar geometry") {
    val geometryValue = SpatialTypeSupport.serializeGeometry(point(4326), GeometryType(4326))
    val geometryChild = Literal(geometryValue, GeometryType(4326))
    val geometryInput = LegacySpatialInput(geometryChild)
    assert(geometryInput.evalWithoutSerialization(InternalRow.empty).isInstanceOf[Geometry])
    assert(SpatialTypeSupport.unwrapGeometryInput(geometryInput) eq geometryChild)
    val geographyValue =
      SpatialTypeSupport.serializeGeography(lazyGeography(4326), GeographyType(4326))
    val geographyChild = Literal(geographyValue, GeographyType(4326))
    val geographyInput = LegacySpatialInput(geographyChild)
    assert(geographyInput.evalWithoutSerialization(InternalRow.empty).isInstanceOf[Geography])
    assert(SpatialTypeSupport.unwrapGeometryInput(geographyInput) eq geographyInput)
  }

  test("SerdeAware geography output stays lazy and validates the declared result CRS") {
    val value = lazyGeography(4326)
    val input = SpatialObject(value, GeographyUDT())
    val result = NativeSpatialResult(input, GeographyType(4326)).eval(InternalRow.empty)
    assert(NativeGeography.fromValue(result.asInstanceOf[BinaryView]).srid() == 4326)
    assertUnparsed(value)
    intercept[SparkRuntimeException] {
      NativeSpatialResult(input, GeographyType(4269)).eval(InternalRow.empty)
    }
  }

  private case class SpatialObject(value: Any, override val dataType: DataType)
      extends LeafExpression
      with CodegenFallback
      with SerdeAware {
    override def nullable: Boolean = value == null
    override def eval(input: InternalRow): Any =
      throw new AssertionError("The native object path must not evaluate legacy SQL bytes")
    override def evalWithoutSerialization(input: InternalRow): Any = value
  }
}
