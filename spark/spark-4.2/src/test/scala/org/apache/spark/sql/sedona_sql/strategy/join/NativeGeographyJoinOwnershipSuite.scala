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

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Arrays
import com.google.common.geometry.S2LatLng
import org.apache.sedona.common.S2Geography.{Geography, GeographyWKBSerializer, PointGeography, WKBGeography}
import org.apache.sedona.common.geography.Constructors
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, LeafExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.sedona_sql.expressions.{InferredExpression, InferrableFunctionConverter}
import org.apache.spark.sql.sedona_sql.expressions.geography.ST_GeogFromEWKB
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType}
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.index.strtree.STRtree
import org.scalatest.funsuite.AnyFunSuite

class NativeGeographyJoinOwnershipSuite extends AnyFunSuite {
  private def row(x: Double): InternalRow = new GenericInternalRow(Array[Any](x))

  private def withLazyGeography(run: => Unit): Unit = {
    val previous = WKBGeography.isEagerShapeIndex
    WKBGeography.setEagerShapeIndex(false)
    try run
    finally WKBGeography.setEagerShapeIndex(previous)
  }

  private def index(geography: Geography): STRtree = {
    val shape = JoinedGeometry.geographyToEnvelopeGeometry(geography)
    shape.setUserData(GeographyJoinShape(geography, new UnsafeRow(0)))
    val result = new STRtree()
    result.insert(shape.getEnvelopeInternal, shape)
    result.build()
    result
  }

  test("retained computed geography owns WKB when its binary child reuses a buffer") {
    withLazyGeography {
      // Exercise the real geography constructor through direct computational evaluation.
      val expression = ST_GeogFromEWKB(Seq(ReusedWkbExpression()))
      val first = TraitJoinQueryBase
        .shapeToGeography(expression, row(1d))
        .asInstanceOf[WKBGeography]
      val bytes = first.getWKBBytes.clone()
      val retained = index(first)
      val second = TraitJoinQueryBase
        .shapeToGeography(expression, row(21d))
        .asInstanceOf[WKBGeography]
      assert(second.getJTSGeometry.getCoordinate.x == 21d)
      assert(Arrays.equals(first.getWKBBytes, bytes))
      val entry = retained
        .query(new org.locationtech.jts.geom.Envelope(0.5d, 1.5d, 1.5d, 2.5d))
        .get(0)
        .asInstanceOf[org.locationtech.jts.geom.Geometry]
      val payload = entry.getUserData.asInstanceOf[GeographyJoinShape]
      assert(payload.geog.asInstanceOf[WKBGeography].getJTSGeometry.getCoordinate.x == 1d)
    }
  }

  test("previous serialized geography evaluation isolates the same reusable binary child") {
    withLazyGeography {
      val expression = ST_GeogFromEWKB(Seq(ReusedWkbExpression()))
      def previousPath(input: InternalRow): WKBGeography = GeographyWKBSerializer
        .deserialize(expression.eval(input).asInstanceOf[Array[Byte]])
        .asInstanceOf[WKBGeography]
      val first = previousPath(row(1d))
      val bytes = first.getWKBBytes.clone()
      val retained = index(first)
      assert(previousPath(row(21d)).getJTSGeometry.getCoordinate.x == 21d)
      assert(Arrays.equals(first.getWKBBytes, bytes))
      val entry = retained
        .query(new org.locationtech.jts.geom.Envelope(0.5d, 1.5d, 1.5d, 2.5d))
        .get(0)
        .asInstanceOf[org.locationtech.jts.geom.Geometry]
      val payload = entry.getUserData.asInstanceOf[GeographyJoinShape]
      assert(payload.geog.asInstanceOf[WKBGeography].getJTSGeometry.getCoordinate.x == 1d)
    }
  }

  for (native <- Seq(false, true)) {
    test(s"retained mutable S2 function results are owned with native adaptation $native") {
      withLazyGeography {
        def expression(): Expression = {
          val children = Seq(BoundReference(0, DoubleType, nullable = false))
          val builder =
            (inputs: Seq[Expression]) => ReusedS2JoinExpression(inputs, new ReusedS2JoinFunction)
          if (native) SpatialTypeSupport.adaptFunction(builder)(children) else builder(children)
        }
        def assertLongitude(geography: Geography, expected: Double): Unit =
          assert(
            math.abs(Constructors.geogToGeometry(geography).getCoordinate.x - expected) < 1e-10)
        def payload(tree: STRtree): Geography = {
          val entries = tree.query(new Envelope(0.5d, 1.5d, 1.5d, 2.5d))
          assert(entries.size() == 1)
          entries.get(0).asInstanceOf[Geometry].getUserData.asInstanceOf[GeographyJoinShape].geog
        }

        val control = ReusedS2JoinExpression(
          Seq(BoundReference(0, DoubleType, nullable = false)),
          new ReusedS2JoinFunction)
        def previous(input: InternalRow): Geography =
          GeographyWKBSerializer.deserialize(control.eval(input).asInstanceOf[Array[Byte]])
        val previousIndex = index(previous(row(1d)))
        assertLongitude(previous(row(21d)), 21d)
        assertLongitude(payload(previousIndex), 1d)

        val source = expression()
        val first = TraitJoinQueryBase.shapeToGeography(source, row(1d))
        val retained = index(first)
        assertLongitude(TraitJoinQueryBase.shapeToGeography(source, row(21d)), 21d)
        assertLongitude(first, 1d)
        assertLongitude(payload(retained), 1d)
      }
    }
  }
}

/** Models a binary computation or UDF whose byte-array result is valid until its next eval. */
private[join] case class ReusedWkbExpression() extends LeafExpression with CodegenFallback {
  private val bytes = ByteBuffer
    .allocate(21)
    .order(ByteOrder.LITTLE_ENDIAN)
    .put(1.toByte)
    .putInt(1)
    .putDouble(0d)
    .putDouble(2d)
    .array()
  override def dataType: DataType = BinaryType
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any = {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putDouble(5, input.getDouble(0))
    bytes
  }
}

/** Exercises the public inferred-function evaluator with a reusable non-WKB result. */
private[join] final class ReusedS2JoinFunction extends (Double => Geography) with Serializable {
  @transient private lazy val value = {
    val geography = new PointGeography()
    geography.setSRID(4326)
    geography
  }
  override def apply(longitude: Double): Geography = {
    value.points.clear()
    value.points.add(S2LatLng.fromDegrees(2d, longitude).toPoint)
    value
  }
}

private[join] case class ReusedS2JoinExpression(
    inputExpressions: Seq[Expression],
    producer: ReusedS2JoinFunction)
    extends InferredExpression(
      InferrableFunctionConverter.inferrableFunction1[Geography, Double](producer)) {
  override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): Expression =
    copy(inputExpressions = children)
}
