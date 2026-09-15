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
package org.apache.spark.sql.execution.datasources.geoparquet

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.sql.execution.datasources.geoparquet.internal.{LegacyBehaviorPolicy, NoopUpdater, RebaseSpec}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{StructField, StructType}
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.{Ordinate, WKBWriter}
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, EnumSet}

class GeoParquetRowConverterTest extends AnyFunSuite {
  test("primitive WKB retains declared empty coordinate layouts through GeometryUDT") {
    val parquetSchema =
      MessageTypeParser.parseMessageType("message root { optional binary geometry; }")
    val catalystSchema = StructType(Seq(StructField("geometry", GeometryUDT(), nullable = true)))
    val metadata = Collections.singletonMap(
      "geo",
      """{"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":[],"crs":null}}}""")
    val converter = new GeoParquetRowConverter(
      new GeoParquetToSparkSchemaConverter(metadata, parameters = Map.empty),
      parquetSchema,
      catalystSchema,
      None,
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED),
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED),
      Map.empty,
      NoopUpdater)
    val factory = new GeometryFactory()

    Seq((3, 0), (3, 1), (4, 1)).foreach { case (dimension, measures) =>
      val sequence = factory.getCoordinateSequenceFactory.create(0, dimension, measures)
      val empty = factory.createPolygon(factory.createLinearRing(sequence))
      val writer = new WKBWriter(dimension)
      val ordinates = EnumSet.of(Ordinate.X, Ordinate.Y)
      if (dimension - measures > 2) ordinates.add(Ordinate.Z)
      if (measures > 0) ordinates.add(Ordinate.M)
      writer.setOutputOrdinates(ordinates)

      converter.start()
      converter
        .getConverter(0)
        .asPrimitiveConverter()
        .addBinary(Binary.fromConstantByteArray(writer.write(empty)))
      converter.end()
      val result = GeometryUDT.deserialize(converter.currentRecord.getBinary(0))
      val resultSequence = result
        .asInstanceOf[org.locationtech.jts.geom.Polygon]
        .getExteriorRing
        .getCoordinateSequence
      assert(resultSequence.getDimension == dimension)
      assert(resultSequence.getMeasures == measures)
    }
  }

  test("legacy byte-array WKB retains declared empty coordinate layouts through GeometryUDT") {
    val parquetSchema = MessageTypeParser.parseMessageType(
      "message root { optional group geometry (LIST) { repeated int32 array (INT_8); } }")
    val catalystSchema = StructType(Seq(StructField("geometry", GeometryUDT(), nullable = true)))
    val metadata = Collections.singletonMap(
      "geo",
      """{"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":[],"crs":null}}}""")
    val parameters = Map("legacyMode" -> "true")
    val converter = new GeoParquetRowConverter(
      new GeoParquetToSparkSchemaConverter(metadata, parameters = parameters),
      parquetSchema,
      catalystSchema,
      None,
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED),
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED),
      parameters,
      NoopUpdater)
    val factory = new GeometryFactory()

    Seq((3, 0), (3, 1), (4, 1)).foreach { case (dimension, measures) =>
      val sequence = factory.getCoordinateSequenceFactory.create(0, dimension, measures)
      val empty = factory.createPolygon(factory.createLinearRing(sequence))
      val writer = new WKBWriter(dimension)
      val ordinates = EnumSet.of(Ordinate.X, Ordinate.Y)
      if (dimension - measures > 2) ordinates.add(Ordinate.Z)
      if (measures > 0) ordinates.add(Ordinate.M)
      writer.setOutputOrdinates(ordinates)

      converter.start()
      val geometryConverter = converter.getConverter(0).asInstanceOf[GroupConverter]
      geometryConverter.start()
      val byteConverter = geometryConverter.getConverter(0).asPrimitiveConverter()
      writer.write(empty).foreach(byte => byteConverter.addInt(byte))
      geometryConverter.end()
      converter.end()
      val result = GeometryUDT.deserialize(converter.currentRecord.getBinary(0))
      val resultSequence = result
        .asInstanceOf[org.locationtech.jts.geom.Polygon]
        .getExteriorRing
        .getCoordinateSequence
      assert(resultSequence.getDimension == dimension)
      assert(resultSequence.getMeasures == measures)
    }
  }
}
