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
package org.apache.spark.sql.sedona_sql.expressions.raster

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.gce.geotiff.{GeoTiffWriteParams, GeoTiffWriter}
import org.opengis.parameter.GeneralParameterValue

import java.io.ByteArrayOutputStream
import javax.imageio.ImageWriteParam

// Expected Types (RasterUDT, StringType, IntegerType) or (RasterUDT, StringType, DecimalType)
case class RS_AsGeoTiff(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback {

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    if (raster == null) return null

    val out = new ByteArrayOutputStream()
    val writer = new GeoTiffWriter(out)
    val gtiffParams = writer.getFormat.getWriteParameters

    // If there are more than one input expressions, the additional ones are used as parameters
    if (inputExpressions.length > 1) {
      val DEFAULT_WRITE_PARAMS: GeoTiffWriteParams = new GeoTiffWriteParams()
      DEFAULT_WRITE_PARAMS.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
      // Available compression types: None, PackBits, Deflate, Huffman, LZW and JPEG
      DEFAULT_WRITE_PARAMS.setCompressionType(inputExpressions(1).asInstanceOf[Literal].value.asInstanceOf[UTF8String].toString)
      // Should be a value between 0 and 1
      // 0 means max compression, 1 means no compression
      inputExpressions(2).eval(input) match {
        case i: Int =>
          DEFAULT_WRITE_PARAMS.setCompressionQuality(i.toFloat)
        case j:Decimal =>
          DEFAULT_WRITE_PARAMS.setCompressionQuality(j.toFloat)
        case _ =>
          throw new IllegalArgumentException("Compression quality must be an integer or decimal value between 0 and 1")
      }
      gtiffParams.parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName.toString).setValue(DEFAULT_WRITE_PARAMS)
    }

    val wps: Array[GeneralParameterValue] = gtiffParams.values.toArray(new Array[GeneralParameterValue](1))
    writer.write(raster, wps)
    writer.dispose()
    out.close()
    out.toByteArray
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}