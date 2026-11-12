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
import org.apache.sedona.common.S2Geography.WKBGeography
import org.apache.sedona.common.geography.Constructors
import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.expressions.st.{ST_GeogFromWKB => SparkGeogFromWKB}
import org.apache.spark.sql.catalyst.util.{Geography => SparkGeography}
import org.apache.spark.sql.sedona_sql.expressions.geography.{ST_GeogFromEWKB, ST_GeogFromEWKT, ST_GeogFromGeoHash, ST_GeomToGeography}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{BinaryType, GeographyType, StringType}
import org.apache.spark.unsafe.types.BinaryView
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import org.locationtech.jts.io.{ByteOrderValues, WKBWriter}
import org.scalatest.funsuite.AnyFunSuite

class NativeGeographyDefaultsSuite extends AnyFunSuite {
  private def wkb(order: Int, srid: Option[Int]): Array[Byte] = {
    val point = new GeometryFactory(new PrecisionModel(), srid.getOrElse(0))
      .createPoint(new Coordinate(1, 2))
    if (srid.isEmpty) new WKBWriter(2, order, false).write(point)
    else {
      // Force an explicit root SRID flag even for SRID 0.
      point.setSRID(4326)
      val bytes = new WKBWriter(2, order, true).write(point)
      ByteBuffer
        .wrap(bytes)
        .order(if (order == ByteOrderValues.LITTLE_ENDIAN) ByteOrder.LITTLE_ENDIAN
        else ByteOrder.BIG_ENDIAN)
        .putInt(5, srid.get)
      bytes
    }
  }
  private def decode(expression: Expression): WKBGeography =
    SpatialTypeSupport
      .deserializeGeography(expression.eval(InternalRow.empty), expression.dataType)
      .asInstanceOf[WKBGeography]
  private def ewkt(value: String): Expression =
    SpatialTypeSupport.adaptFunction(ST_GeogFromEWKT.apply)(
      Seq(Literal.create(value, StringType)))
  private def ewkb(value: Array[Byte]): Expression =
    SpatialTypeSupport.adaptFunction(ST_GeogFromEWKB.apply)(Seq(Literal(value, BinaryType)))

  test("native EWKT defaults only a genuinely absent CRS to 4326") {
    assert(decode(ewkt("POINT (1 2)")).getSRID == 4326)
    assert(decode(ewkt("SRID=4326;POINT (1 2)")).getSRID == 4326)
    assert(decode(ewkt("SRID=4269;POINT (1 2)")).getSRID == 4269)
    intercept[SparkIllegalArgumentException] { decode(ewkt("SRID=0;POINT (1 2)")) }
    intercept[SparkIllegalArgumentException] { decode(ewkt("SRID=3857;POINT (1 2)")) }
  }

  test("native EWKB defaults missing CRS but preserves explicit CRS in both byte orders") {
    Seq(ByteOrderValues.LITTLE_ENDIAN, ByteOrderValues.BIG_ENDIAN).foreach { order =>
      assert(decode(ewkb(wkb(order, None))).getSRID == 4326)
      assert(decode(ewkb(wkb(order, Some(4326)))).getSRID == 4326)
      assert(decode(ewkb(wkb(order, Some(4269)))).getSRID == 4269)
      intercept[SparkIllegalArgumentException] { decode(ewkb(wkb(order, Some(0)))) }
      intercept[SparkIllegalArgumentException] { decode(ewkb(wkb(order, Some(3857)))) }
    }
  }

  test("common legacy constructors retain absent and explicit zero SRIDs") {
    assert(Constructors.geogFromEWKT("POINT (1 2)").getSRID == 0)
    assert(Constructors.geogFromEWKT("SRID=0;POINT (1 2)").getSRID == 0)
    assert(Constructors.geogFromWKB(wkb(ByteOrderValues.LITTLE_ENDIAN, None)).getSRID == 0)
    assert(Constructors.geogFromWKB(wkb(ByteOrderValues.LITTLE_ENDIAN, Some(0))).getSRID == 0)
  }

  test(
    "per-row defaulting distinguishes absent and explicit zero without analysis-time guessing") {
    val textExpr = SpatialTypeSupport.adaptFunction(ST_GeogFromEWKT.apply)(
      Seq(BoundReference(0, StringType, nullable = true)))
    val binaryExpr = SpatialTypeSupport.adaptFunction(ST_GeogFromEWKB.apply)(
      Seq(BoundReference(0, BinaryType, nullable = true)))
    def textRow(value: String) = new GenericInternalRow(Array[Any](UTF8String.fromString(value)))
    def bytesRow(value: Array[Byte]) = new GenericInternalRow(Array[Any](value))
    val textValue = textExpr.eval(textRow("POINT (1 2)"))
    assert(SpatialTypeSupport.deserializeGeography(textValue, textExpr.dataType).getSRID == 4326)
    intercept[SparkIllegalArgumentException] { textExpr.eval(textRow("SRID=0;POINT (1 2)")) }
    val binaryValue = binaryExpr.eval(bytesRow(wkb(ByteOrderValues.LITTLE_ENDIAN, None)))
    assert(
      SpatialTypeSupport.deserializeGeography(binaryValue, binaryExpr.dataType).getSRID == 4326)
    intercept[SparkIllegalArgumentException] {
      binaryExpr.eval(bytesRow(wkb(ByteOrderValues.LITTLE_ENDIAN, Some(0))))
    }
    assert(textExpr.eval(textRow(null)) == null)
    assert(binaryExpr.eval(bytesRow(null)) == null)
  }

  test("converting a geometry with explicit SRID zero does not acquire a default geography CRS") {
    val point = new GeometryFactory().createPoint(new Coordinate(1, 2))
    val expr = SpatialTypeSupport.adaptFunction(ST_GeomToGeography.apply)(
      Seq(BoundReference(0, GeometryUDT(), nullable = false)))
    val row = new GenericInternalRow(
      Array[Any](SpatialTypeSupport.serializeGeometry(point, GeometryUDT())))
    intercept[SparkIllegalArgumentException] { expr.eval(row) }
  }

  test("GeoHash already has geographic CRS and needs no adaptation") {
    assert(Constructors.geogFromGeoHash("9q9j", null).getSRID == 4326)
    val expr = SpatialTypeSupport.adaptFunction(ST_GeogFromGeoHash.apply)(
      Seq(Literal("9q9j"), Literal.create(null, org.apache.spark.sql.types.IntegerType)))
    assert(decode(expr).getSRID == 4326)
  }

  test("nulls and malformed inputs retain null and failure behavior") {
    assert(ewkt(null).eval(InternalRow.empty) == null)
    assert(ewkb(null).eval(InternalRow.empty) == null)
    intercept[Exception] { decode(ewkt("SRID=4326 POINT (1 2)")) }
    intercept[Exception] { decode(ewkb(Array[Byte](1, 1, 0, 0, 32))) }
  }

  test("native EWKB construction retains lazy WKB bytes without materializing S2 or JTS") {
    val eagerShapeIndex = WKBGeography.isEagerShapeIndex
    WKBGeography.setEagerShapeIndex(false)
    try {
      val decoded = decode(ewkb(wkb(ByteOrderValues.LITTLE_ENDIAN, None)))
      Seq("jtsGeometry", "s2Geography", "shapeIndexGeography").foreach { name =>
        val field = classOf[WKBGeography].getDeclaredField(name)
        field.setAccessible(true)
        assert(field.get(decoded) == null, name)
      }
      assert(decoded.getJTSGeometry.getCoordinate.equals2D(new Coordinate(1, 2)))
    } finally {
      WKBGeography.setEagerShapeIndex(eagerShapeIndex)
    }
  }

  test("Spark builtin unary geography WKB constructor still owns the default contract") {
    val expr = SparkGeogFromWKB(Literal(wkb(ByteOrderValues.LITTLE_ENDIAN, None)))
    assert(expr.dataType == GeographyType(4326))
    val value = expr.replacement.eval(InternalRow.empty).asInstanceOf[BinaryView]
    assert(SparkGeography.fromValue(value).srid() == 4326)
  }
}
