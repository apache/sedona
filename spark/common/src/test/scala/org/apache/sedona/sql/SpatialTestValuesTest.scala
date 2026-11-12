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
package org.apache.sedona.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, Geometry, GeometryFactory, LineString, Point, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite

class SpatialTestValuesTest extends AnyFunSuite {
  private val factory = new GeometryFactory(new PrecisionModel(), 3857)
  private val point = factory.createPoint(new Coordinate(1, 2))

  test("legacy geometry values retain identity and null results remain null") {
    assert(SpatialTestValues.geometry(point) eq point)
    assert(SpatialTestValues.geometry(Row(point), 0) eq point)
    assert(SpatialTestValues.geometry(null) == null)
    assert(SpatialTestValues.geometry(Row(null), 0) == null)
  }

  test("default external geometry rows retain SRID and support index and field access") {
    val value = SpatialTypeSupport.externalGeometry(point)
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("a.geom`name", SpatialTypeSupport.geometryType)))
    val row = new GenericRowWithSchema(Array[Any](7, value), schema)
    if (!value.isInstanceOf[Geometry]) {
      // The previous direct JTS cast cannot read Spark's external native value.
      intercept[ClassCastException](row.getAs[Geometry](1).getSRID)
    }
    Seq(SpatialTestValues.geometry(row, 1), SpatialTestValues.geometry(row, "a.geom`name"))
      .foreach { actual =>
        assert(actual.equalsExact(point))
        assert(actual.getSRID == 3857)
        assert(actual.getFactory.getSRID == 3857)
      }
    val nullRow = new GenericRowWithSchema(Array[Any](7, null), schema)
    assert(SpatialTestValues.geometry(nullRow, "a.geom`name") == null)
  }

  test("external result access preserves ZM coordinates and typed empty sequences") {
    val line = factory.createLineString(
      Array[Coordinate](new CoordinateXYZM(1, 2, 3, 4), new CoordinateXYZM(5, 6, 7, 8)))
    val result = SpatialTestValues
      .geometry(SpatialTypeSupport.externalGeometry(line))
      .asInstanceOf[LineString]
    assert(result.getSRID == 3857)
    assert(result.getCoordinateSequence.getDimension == 4)
    assert(result.getCoordinateSequence.getMeasures == 1)
    assert(result.getCoordinateSequence.getZ(1) == 7d)
    assert(result.getCoordinateSequence.getM(1) == 8d)
    val empty = factory.createPoint(factory.getCoordinateSequenceFactory.create(0, 4, 1))
    val emptyResult = SpatialTestValues
      .geometry(SpatialTypeSupport.externalGeometry(empty))
      .asInstanceOf[Point]
    assert(emptyResult.isEmpty)
    assert(emptyResult.getSRID == 3857)
    assert(emptyResult.getCoordinateSequence.getDimension == 4)
    assert(emptyResult.getCoordinateSequence.getMeasures == 1)
  }

  test("geography result access preserves legacy identity, nulls, and named fields") {
    val geography =
      org.apache.sedona.common.geography.Constructors.geogFromWKT("POINT (1 2)", 4326)
    val schema = StructType(Seq(StructField("geog", GeographyUDT())))
    val row = new GenericRowWithSchema(Array[Any](geography), schema)
    assert(SpatialTestValues.geography(geography) eq geography)
    assert(SpatialTestValues.geography(row, 0) eq geography)
    assert(SpatialTestValues.geography(row, "geog") eq geography)
    assert(SpatialTestValues.geography(null) == null)
    assert(SpatialTestValues.geography(Row(null), 0) == null)
    intercept[IllegalArgumentException](SpatialTestValues.geography("POINT (1 2)"))
    intercept[IllegalArgumentException](SpatialTestValues.geography(Array[Byte](1, 2, 3)))
  }

  test("geometry result access does not reinterpret binary or scalar values") {
    intercept[RuntimeException](SpatialTestValues.geometry(Array[Byte](1, 2, 3)))
    intercept[RuntimeException](SpatialTestValues.geometry("POINT (1 2)"))
  }
}
