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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.common.S2Geography.WKBGeography
import org.apache.sedona.common.geography.{Constructors => GeographyConstructors}
import org.apache.sedona.common.raster.RasterConstructors
import org.apache.sedona.sql.utils.RasterSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.sedona_sql.expressions.{SerdeAware, ST_Buffer, ST_Intersects, ST_Transform}
import org.apache.spark.sql.sedona_sql.expressions.raster.RS_Envelope
import org.apache.spark.sql.sedona_sql.UDT.{GeographyUDT, GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.types.{LegacySpatialInput, SpatialTypeSupport}
import org.apache.spark.sql.types.{DataType, GeographyType, GeometryType}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite

/** Pure row-boundary tests; the RDD/broadcast execution tests are in NativeJoinExecutionSuite. */
class NativeJoinBoundarySuite extends AnyFunSuite {
  private def point(x: Double, y: Double, srid: Int): Geometry =
    new GeometryFactory(new PrecisionModel(), srid).createPoint(new Coordinate(x, y))

  private def nativeGeometry(value: Geometry): Expression = {
    val dataType = GeometryType(if (value == null) 4326 else value.getSRID)
    new RejectLegacyJoinInput(
      Literal(SpatialTypeSupport.serializeGeometry(value, dataType), dataType))
  }

  test("native geometry join input bypasses poisoned SQL-byte eval and retains null contracts") {
    val expression = nativeGeometry(point(1, 2, 4326))
    val decoded = TraitJoinQueryBase.shapeToGeometry(expression, InternalRow.empty)
    assert(decoded.getCoordinate.x == 1d)
    assert(decoded.getCoordinate.y == 2d)
    assert(decoded.getSRID == 4326)
    assert(decoded.getFactory.getSRID == 4326)
    val missing = nativeGeometry(null)
    assert(TraitJoinQueryBase.shapeToGeometry(missing, InternalRow.empty) == null)
    assert(TraitJoinQueryBase.shapeToGeometryOrEmpty(missing, InternalRow.empty).isEmpty)
  }

  test("raster candidate conversion decodes projected native geometry without legacy SQL bytes") {
    val projected = point(111319.49079327357, 111325.1428663851, 3857)
    val envelope =
      TraitJoinQueryBase.shapeToWGS84Envelope(nativeGeometry(projected), InternalRow.empty)
    assert(math.abs(envelope.getCentroid.getX - 1d) < 1e-7)
    assert(math.abs(envelope.getCentroid.getY - 1d) < 1e-7)
    assert(
      TraitJoinQueryBase.shapeToWGS84Envelope(nativeGeometry(null), InternalRow.empty) == null)
  }

  test("raster serialization and explicit geometry UDT controls remain unchanged") {
    val raster = RasterConstructors.makeEmptyRaster(1, "D", 2, 2, 0d, 2d, 1d, -1d, 0d, 0d, 4326)
    val rasterExpression = Literal(RasterSerializer.serialize(raster), RasterUDT())
    val actual = TraitJoinQueryBase.shapeToWGS84Envelope(rasterExpression, InternalRow.empty)
    val expected = JoinedGeometryRaster.rasterToWGS84Envelope(raster)
    assert(actual.equalsExact(expected))
    val geometry = point(3, 4, 0)
    val legacy =
      Literal(SpatialTypeSupport.serializeGeometry(geometry, GeometryUDT()), GeometryUDT())
    assert(TraitJoinQueryBase.shapeToGeometry(legacy, InternalRow.empty).equalsExact(geometry))
  }

  test("computed buffer, transform and raster envelope shapes bypass their SQL serializer") {
    val input = nativeGeometry(point(1, 2, 4326))
    val buffer = ST_Buffer(Seq(input, Literal(0.1d)))
    val computed = new RejectComputedJoinSerialization(buffer)
    val result = TraitJoinQueryBase.shapeToGeometry(computed, InternalRow.empty)
    assert(
      result.equalsExact(
        buffer.evalWithoutSerialization(InternalRow.empty).asInstanceOf[Geometry]))
    val transform =
      ST_Transform(Seq(input, Literal("EPSG:4326"), Literal("EPSG:3857")), false, "", "", "")
    val projected = TraitJoinQueryBase.shapeToGeometry(
      new RejectComputedJoinSerialization(transform),
      InternalRow.empty)
    assert(math.abs(projected.getCoordinate.x - 111319.49079327357) < 1e-6)
    val raster = RasterConstructors.makeEmptyRaster(1, "D", 2, 2, 0d, 2d, 1d, -1d, 0d, 0d, 4326)
    val envelope = RS_Envelope(Seq(Literal(RasterSerializer.serialize(raster), RasterUDT())))
    assert(
      TraitJoinQueryBase
        .shapeToGeometry(new RejectComputedJoinSerialization(envelope), InternalRow.empty)
        .equalsExact(envelope.evalWithoutSerialization(InternalRow.empty).asInstanceOf[Geometry]))
    val missing =
      new RejectComputedJoinSerialization(ST_Buffer(Seq(nativeGeometry(null), Literal(0.1d))))
    assert(TraitJoinQueryBase.shapeToGeometry(missing, InternalRow.empty) == null)
  }

  test("native registration retains computed validation without legacy join serialization") {
    val nativeInput = nativeGeometry(point(1, 2, 4326)).children.head
    val buffer = SpatialTypeSupport.adaptFunction(children => ST_Buffer(children))(
      Seq(nativeInput, Literal(0.1d)))
    assert(SpatialTypeSupport.isNativeGeometry(buffer.dataType))
    val predicate = SpatialTypeSupport.adaptFunction(children => ST_Intersects(children))(
      Seq(buffer, nativeInput))
    val candidate = predicate.children.head
    assert(candidate.isInstanceOf[LegacySpatialInput])
    assert(SpatialTypeSupport.unwrapGeometryInput(candidate) eq buffer)
    assert(candidate.isInstanceOf[SerdeAware])
    assert(
      TraitJoinQueryBase
        .shapeToGeometry(new RejectComputedJoinSerialization(candidate), InternalRow.empty)
        .contains(point(1, 2, 4326)))
  }

  test("computed shape ownership is isolated before a join attaches its row payload") {
    val reusable = point(1, 2, 4326)
    reusable.setUserData("expression-owner")
    val expression = new ReusedGeometryJoinExpression(reusable)
    val first = TraitJoinQueryBase.shapeToGeometry(expression, InternalRow.empty)
    val second = TraitJoinQueryBase.shapeToGeometry(expression, InternalRow.empty)
    assert(first ne reusable)
    assert(second ne first)
    first.setUserData("row-one")
    second.setUserData("row-two")
    assert(reusable.getUserData == "expression-owner")
    assert(first.getUserData == "row-one")
  }

  test("geography has a distinct direct decoder and an explicit planar candidate conversion") {
    val value = GeographyConstructors.geogFromWKT("POINT (1 2)", 4326)
    val child = Literal(
      SpatialTypeSupport.serializeGeography(value, GeographyType(4326)),
      GeographyType(4326))
    val expression = new RejectLegacyJoinInput(child)
    assert(SpatialTypeSupport.unwrapGeometryInput(expression) eq expression)
    assert(SpatialTypeSupport.unwrapGeographyInput(expression) eq child)
    val decoded = TraitJoinQueryBase.shapeToGeography(expression, InternalRow.empty)
    assert(decoded.isInstanceOf[WKBGeography])
    assert(decoded.getSRID == 4326)
    val geometry = TraitJoinQueryBase.shapeToGeometry(expression, InternalRow.empty)
    assert(geometry.getCoordinate.x == 1d && geometry.getCoordinate.y == 2d)
    assert(geometry.getSRID == 4326)
    val legacy =
      Literal(SpatialTypeSupport.serializeGeography(value, GeographyUDT()), GeographyUDT())
    assert(TraitJoinQueryBase.shapeToGeometry(legacy, InternalRow.empty).equalsExact(geometry))
    val missing = new RejectLegacyJoinInput(Literal(null, GeographyType(4326)))
    assert(TraitJoinQueryBase.shapeToGeography(missing, InternalRow.empty) == null)
    assert(TraitJoinQueryBase.shapeToGeometry(missing, InternalRow.empty) == null)
    val buffer = ST_Buffer(Seq(expression, Literal(100d)))
    val computed = new RejectComputedJoinSerialization(buffer)
    val buffered = TraitJoinQueryBase.shapeToGeography(computed, InternalRow.empty)
    assert(org.apache.sedona.common.geography.Functions.contains(buffered, value))
    assert(!TraitJoinQueryBase.shapeToGeometry(computed, InternalRow.empty).isEmpty)
  }
}

/** Tests the real computation while poisoning its normal serializer-based eval path. */
private[join] case class RejectComputedJoinSerialization(override val child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with SerdeAware {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def eval(input: InternalRow): Any =
    throw new AssertionError("Computed join shape evaluated its SQL serializer")
  override def evalWithoutSerialization(input: InternalRow): Any =
    child.asInstanceOf[SerdeAware].evalWithoutSerialization(input)
  override protected def withNewChildInternal(newChild: Expression): Expression =
    new RejectComputedJoinSerialization(newChild)
}

private[join] case class ReusedGeometryJoinExpression(value: Geometry)
    extends org.apache.spark.sql.catalyst.expressions.LeafExpression
    with CodegenFallback
    with SerdeAware {
  override def dataType: DataType = GeometryUDT()
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any =
    throw new AssertionError("Reusable join geometry evaluated its SQL serializer")
  override def evalWithoutSerialization(input: InternalRow): Any = value
}

/** Survives BindReferences and throws if an executor serializes through the legacy adapter. */
private[join] class RejectLegacyJoinInput(override val child: Expression)
    extends LegacySpatialInput(child) {
  override def eval(input: InternalRow): Any =
    throw new AssertionError("Native join input evaluated the legacy SQL-byte adapter")
  override protected def withNewChildInternal(newChild: Expression): Expression =
    new RejectLegacyJoinInput(newChild)
}
