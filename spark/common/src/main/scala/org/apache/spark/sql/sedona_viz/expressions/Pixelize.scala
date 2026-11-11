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
package org.apache.spark.sql.sedona_viz.expressions

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.sedona.viz.core.Serde.PixelSerializer
import org.apache.sedona.viz.utils.{ColorizeOption, Pixel, RasterizationUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sedona_viz.UDT.PixelUDT
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.locationtech.jts.geom._

import scala.jdk.CollectionConverters._

case class ST_Pixelize(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback
    with Logging {
  assert(inputExpressions.length <= 5)
  override def toString: String = s" **${ST_Pixelize.getClass.getName}**  "

  override def eval(input: InternalRow): Any = {
    val inputGeometry =
      GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[Array[Byte]])
    val resolutionX = inputExpressions(1).eval(input).asInstanceOf[Integer]
    val resolutionY = inputExpressions(2).eval(input).asInstanceOf[Integer]
    val boundary = GeometrySerializer
      .deserialize(inputExpressions(3).eval(input).asInstanceOf[Array[Byte]])
      .getEnvelopeInternal
    val pixels = Pixelizer.pixelize(inputGeometry, resolutionX, resolutionY, boundary)

    return new GenericArrayData(
      pixels.asScala
        .map(f => {
          val out = new ByteArrayOutputStream()
          val kryo = new Kryo()
          val pixelSerializer = new PixelSerializer()
          val output = new Output(out)
          pixelSerializer.write(kryo, output, f._1)
          output.close()
          new GenericArrayData(out.toByteArray)
        })
        .toArray)
  }
  override def dataType: DataType = ArrayType(new PixelUDT)
  override def children: Seq[Expression] = inputExpressions

  override def nullable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[expressions] object Pixelizer {

  private val reverseCoordinate = false

  /**
   * Converts a geometry to the pixels it covers on a resolutionX by resolutionY grid laid over
   * the boundary. Collections, including the Multi* types, are converted member by member. An
   * empty geometry covers no pixels.
   *
   * Lines and polygon rings are traced segment by segment with Bresenham's algorithm, which draws
   * a segment's start pixel but not its end pixel, so a segment whose two ends fall in the same
   * pixel draws nothing. When every vertex of a line or polygon falls in one pixel, nothing at
   * all is drawn, so the geometry is given the single pixel that contains it instead.
   */
  def pixelize(
      geometry: Geometry,
      resolutionX: Int,
      resolutionY: Int,
      boundary: Envelope): java.util.List[(Pixel, java.lang.Double)] = {
    if (geometry.isEmpty) {
      return new java.util.ArrayList[(Pixel, java.lang.Double)]()
    }
    geometry match {
      case point: Point =>
        RasterizationUtils.FindPixelCoordinates(
          resolutionX,
          resolutionY,
          boundary,
          point,
          ColorizeOption.NORMAL,
          reverseCoordinate)
      case line: LineString =>
        orContainingPixel(
          RasterizationUtils
            .FindPixelCoordinates(resolutionX, resolutionY, boundary, line, reverseCoordinate),
          line,
          resolutionX,
          resolutionY,
          boundary)
      case polygon: Polygon =>
        orContainingPixel(
          RasterizationUtils
            .FindPixelCoordinates(resolutionX, resolutionY, boundary, polygon, reverseCoordinate),
          polygon,
          resolutionX,
          resolutionY,
          boundary)
      case collection: GeometryCollection =>
        val pixels = new java.util.ArrayList[(Pixel, java.lang.Double)]()
        for (i <- 0 until collection.getNumGeometries) {
          pixels.addAll(pixelize(collection.getGeometryN(i), resolutionX, resolutionY, boundary))
        }
        pixels
    }
  }

  /**
   * A non-empty line or polygon traces to no pixels only when all of its vertices fall in the
   * same pixel. Return that pixel rather than nothing.
   */
  private def orContainingPixel(
      traced: java.util.List[(Pixel, java.lang.Double)],
      geometry: Geometry,
      resolutionX: Int,
      resolutionY: Int,
      boundary: Envelope): java.util.List[(Pixel, java.lang.Double)] = {
    if (!traced.isEmpty) {
      return traced
    }
    RasterizationUtils.FindPixelCoordinates(
      resolutionX,
      resolutionY,
      boundary,
      geometry.getFactory.createPoint(geometry.getCoordinate),
      ColorizeOption.NORMAL,
      reverseCoordinate)
  }
}
