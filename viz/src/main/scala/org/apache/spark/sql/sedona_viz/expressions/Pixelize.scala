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

import java.io.ByteArrayOutputStream
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import org.apache.sedona.core.serde.GeometrySerde
import org.apache.sedona.viz.core.Serde.PixelSerializer
import org.apache.sedona.viz.utils.{ColorizeOption, RasterizationUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.sedona_viz.UDT.PixelUDT
import org.apache.spark.sql.types.StructType
import org.locationtech.jts.geom._

case class ST_Pixelize(inputExpressions: Seq[Expression])
  extends Generator with CodegenFallback with Logging {
  override def elementSchema: StructType = new StructType().add("Pixel", new PixelUDT)

  override def toString: String = s" **${ST_Pixelize.getClass.getName}**  "

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    assert(inputExpressions.length <= 5)
    val inputGeometry = GeometrySerde.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val resolutionX = inputExpressions(1).eval(input).asInstanceOf[Integer]
    val resolutionY = inputExpressions(2).eval(input).asInstanceOf[Integer]
    val boundary = GeometrySerde.deserialize(inputExpressions(3).eval(input).asInstanceOf[ArrayData]).getEnvelopeInternal
    val reverseCoordinate = false
    val pixels = inputGeometry match {
      case geometry: LineString => {
        RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, inputGeometry.asInstanceOf[LineString], reverseCoordinate)
      }
      case geometry: Polygon => {
        RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, inputGeometry.asInstanceOf[Polygon], reverseCoordinate)
      }
      case geometry: Point => {
        RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, inputGeometry.asInstanceOf[Point], ColorizeOption.NORMAL, reverseCoordinate)
      }
      case geometry: MultiLineString => {
        var manyPixels = RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, geometry.getGeometryN(0).asInstanceOf[LineString], reverseCoordinate)
        for (i <- 1 to geometry.getNumGeometries - 1) {
          manyPixels.addAll(RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, geometry.getGeometryN(i).asInstanceOf[LineString], reverseCoordinate))
        }
        manyPixels
      }
      case geometry: MultiPolygon => {
        var manyPixels = RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, geometry.getGeometryN(0).asInstanceOf[Polygon], reverseCoordinate)
        for (i <- 1 to geometry.getNumGeometries - 1) {
          manyPixels.addAll(RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, geometry.getGeometryN(i).asInstanceOf[Polygon], reverseCoordinate))
        }
        manyPixels
      }
      case geometry: MultiPoint => {
        var manyPixels = RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, geometry.getGeometryN(0).asInstanceOf[Point], ColorizeOption.NORMAL, reverseCoordinate)
        for (i <- 1 to geometry.getNumGeometries - 1) {
          manyPixels.addAll(RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, boundary, geometry.getGeometryN(i).asInstanceOf[Point], ColorizeOption.NORMAL, reverseCoordinate))
        }
        manyPixels
      }
    }
    assert(pixels.size() > 0)
    val resultRows = new Array[InternalRow](pixels.size())
    val pixelSerializer = new PixelSerializer()
    for (i <- 0 to pixels.size() - 1) {
      val out = new ByteArrayOutputStream()
      val kryo = new Kryo()
      val pixelSerializer = new PixelSerializer()
      val output = new Output(out)
      pixelSerializer.write(kryo, output, pixels.get(i)._1)
      output.close()
      resultRows(i) = InternalRow(new GenericArrayData(out.toByteArray))
    }
    return resultRows
  }

  override def children: Seq[Expression] = inputExpressions
}
