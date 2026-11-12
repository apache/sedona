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

import org.apache.sedona.sql.utils.{GeographySerializer, GeometrySerializer}
import org.apache.sedona.common.S2Geography.WKBGeography
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sedona_sql.UDT.{GeographyUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions.{DataframePhysicalFunction, ST_BinaryDistanceBandColumn, ST_Contains, ST_WeightedDistanceBandColumn}
import org.apache.spark.sql.sedona_sql.optimization.SpatialFilterPushDownForGeoParquet
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import org.locationtech.jts.io.WKBReader
import org.scalatest.funsuite.AnyFunSuite

class NativeResidualBoundarySuite extends AnyFunSuite {
  test("explicit UDT schemas name the legacy Python codec classes on native Spark") {
    assert(GeometryUDT.pyUDT == "sedona.spark.sql.types.LegacyGeometryType")
    assert(GeographyUDT.pyUDT == "sedona.spark.sql.types.LegacyGeographyType")
    assert(GeometryUDT.sqlType == BinaryType)
    assert(GeographyUDT.sqlType == BinaryType)
    assert(GeometryUDT.json.contains("LegacyGeometryType"))
    assert(GeographyUDT.json.contains("LegacyGeographyType"))
  }

  for ((srid, prefix) <- Seq(0 -> "00000000", 4326 -> "e6100000")) {
    test(s"explicit geography UDT preserves SRID $srid in an independent little-endian fixture") {
      val wkb = WKBReader.hexToBytes("0101000000000000000000f03f0000000000000040")
      val fixture = WKBReader.hexToBytes(prefix) ++ wkb
      val geography = WKBGeography.fromWKB(wkb, srid)
      val explicit = GeographyUDT()
      val encoded = SpatialTypeSupport.serializeGeography(geography, explicit)
      assert(encoded.asInstanceOf[Array[Byte]].sameElements(fixture))
      assert(explicit.serialize(geography).sameElements(fixture))
      Seq(
        SpatialTypeSupport.deserializeGeography(fixture, explicit),
        explicit.deserialize(fixture)).foreach { decoded =>
        assert(decoded.getSRID == srid)
        assert(decoded.asInstanceOf[WKBGeography].getJTSGeometry.toText == "POINT (1 2)")
      }
    }
  }

  test(
    "nested native and explicit UDT geography conversions preserve little-endian bytes and nulls") {
    val fixture = WKBReader.hexToBytes("e61000000101000000000000000000f03f0000000000000040")
    val nativeType = StructType(
      Seq(
        StructField("values", ArrayType(GeographyType(4326), containsNull = true)),
        StructField("optional", GeographyType(4326))))
    val legacyType = SpatialTypeSupport.legacyType(nativeType)
    val legacy =
      new GenericInternalRow(Array[Any](new GenericArrayData(Array[Any](fixture, null)), null))
    val native = SpatialTypeSupport.toNativeValue(legacy, legacyType).asInstanceOf[InternalRow]
    val values = native.getArray(0)
    val geography = SpatialTypeSupport.geography(values.get(0, GeographyType(4326)))
    assert(geography.getSRID == 4326)
    assert(geography.asInstanceOf[WKBGeography].getJTSGeometry.toText == "POINT (1 2)")
    assert(values.isNullAt(1))
    assert(native.isNullAt(1))

    val restored = SpatialTypeSupport.toLegacyValue(native, nativeType).asInstanceOf[InternalRow]
    assert(restored.getArray(0).getBinary(0).sameElements(fixture))
    assert(restored.getArray(0).isNullAt(1))
    assert(restored.isNullAt(1))
    assert(SpatialTypeSupport.toNativeValue(null, legacyType) == null)
    assert(SpatialTypeSupport.toLegacyValue(null, nativeType) == null)
  }

  test("physical distance-band functions retain the actual native neighbor payload schema") {
    val shape = AttributeReference("geometry", GeometryType(4326))()
    val id = AttributeReference("id", IntegerType)()
    val attributes = CreateStruct(Seq(shape, id))
    val binary = SpatialTypeSupport.adaptFunction(ST_BinaryDistanceBandColumn.apply)(
      Seq(shape, Literal(1d), Literal(true), Literal(false), Literal(false), attributes))
    val weighted = SpatialTypeSupport.adaptFunction(ST_WeightedDistanceBandColumn.apply)(
      Seq(
        shape,
        Literal(1d),
        Literal(-1d),
        Literal(true),
        Literal(false),
        Literal(1d),
        Literal(false),
        attributes))
    Seq(binary, weighted).foreach { expression =>
      val physical = expression.collectFirst { case p: DataframePhysicalFunction => p }.get
      assert(physical.children.last.dataType == attributes.dataType)
      val result = physical.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      assert(result("neighbor").dataType == attributes.dataType)
      assert(physical.checkInputDataTypes().isSuccess)
    }
  }

  test("physical container schema retains explicit UDT fields alongside native fields") {
    val shape = AttributeReference("geometry", GeometryType(4326))()
    val explicit = AttributeReference("legacy", GeometryUDT())()
    val attributes = CreateStruct(Seq(shape, explicit))
    val expression = SpatialTypeSupport.adaptFunction(ST_BinaryDistanceBandColumn.apply)(
      Seq(shape, Literal(1d), Literal(true), Literal(false), Literal(false), attributes))
    val physical = expression.collectFirst { case p: DataframePhysicalFunction => p }.get
    assert(physical.children.last.dataType == attributes.dataType)
    val result = physical.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    assert(result("neighbor").dataType == attributes.dataType)
  }

  test("GeoParquet pushdown sees through native geometry input adapters only") {
    val point = new GeometryFactory(new PrecisionModel(), 4326).createPoint(new Coordinate(1, 2))
    val literal = Literal.create(GeometrySerializer.serialize(point), GeometryUDT())
    val translator = new SpatialFilterPushDownForGeoParquet(null)
    val geometry = AttributeReference("shape", GeometryType(4326))()
    val nativePredicate =
      SpatialTypeSupport.adaptFunction(ST_Contains.apply)(Seq(geometry, literal))
    assert(translator.translateToGeoParquetSpatialFilters(Seq(nativePredicate)).size == 1)
    val explicit = AttributeReference("shape", GeometryUDT())()
    val legacyPredicate = ST_Contains(Seq(explicit, literal))
    assert(translator.translateToGeoParquetSpatialFilters(Seq(legacyPredicate)).size == 1)
    val geography = AttributeReference("globe", GeographyType(4326))()
    val geographyLiteral =
      Literal.create(GeographySerializer.serialize(WKBGeography.fromJTS(point)), GeographyUDT())
    val spherical =
      SpatialTypeSupport.adaptFunction(ST_Contains.apply)(Seq(geography, geographyLiteral))
    assert(translator.translateToGeoParquetSpatialFilters(Seq(spherical)).isEmpty)
  }
}
