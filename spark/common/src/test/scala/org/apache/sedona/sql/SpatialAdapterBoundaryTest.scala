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

import org.apache.sedona.common.S2Geography.WKBGeography
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.util.DfUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.sedona_sql.UDT.{GeographyUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.sedona_sql.adapters.StructuredAdapter
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, PrecisionModel}

class SpatialAdapterBoundaryTest extends TestBaseScala {
  private def point(x: Double): Geometry =
    new GeometryFactory(new PrecisionModel(), 4326).createPoint(new Coordinate(x, 2))

  describe("spatial adapter boundaries") {
    it("preserves the default geometry schema and string attributes through Adapter") {
      val geometry = point(1)
      geometry.setUserData("alpha\t")
      val spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = sc.parallelize(Seq(geometry)).toJavaRDD()
      val frame = Adapter.toDf(spatialRDD, Seq("label", "empty"), sparkSession)
      assert(
        frame.schema == StructType(
          Seq(
            StructField("geometry", SpatialTypeSupport.geometryType),
            StructField("label", StringType),
            StructField("empty", StringType))))
      val row = frame.head()
      assert(SpatialTestValues.geometry(row, 0).getSRID == 4326)
      assert(row.getString(1) == "alpha")
      assert(row.getString(2) == "")
      val restored = Adapter.toSpatialRdd(frame, "geometry").rawSpatialRDD.first()
      assert(restored.equalsExact(geometry))
      assert(restored.getUserData == "alpha\t")
      assert(geometry.getUserData == "alpha\t")
    }

    it("preserves an explicitly requested GeometryUDT schema") {
      val geometry = point(3)
      geometry.setUserData("7")
      val spatialRDD = new SpatialRDD[Geometry]
      spatialRDD.rawSpatialRDD = sc.parallelize(Seq(geometry)).toJavaRDD()
      val schema =
        StructType(Seq(StructField("shape", GeometryUDT()), StructField("number", IntegerType)))
      val frame = Adapter.toDf(spatialRDD, schema, sparkSession)
      assert(frame.schema == schema)
      assert(frame.head().getAs[Geometry](0).equalsExact(geometry))
      assert(frame.head().getInt(1) == 7)
    }

    it("keeps full rows and geography values through StructuredAdapter") {
      val geometry = point(4)
      val geography = WKBGeography.fromJTS(point(5))
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("shape", GeometryUDT()),
          StructField("globe", GeographyUDT()),
          StructField("label", StringType)))
      val source = sparkSession.createDataFrame(
        sc.parallelize(Seq(Row(7, geometry, geography, null))),
        schema)
      val spatialRDD = StructuredAdapter.toSpatialRdd(source)
      val converted = StructuredAdapter.toDf(spatialRDD, sparkSession)
      assert(converted.schema == schema)
      val row = converted.head()
      assert(row.getInt(0) == 7)
      assert(row.getAs[Geometry](1).equalsExact(geometry))
      assert(row.getAs[WKBGeography](2).getSRID == 4326)
      assert(row.getAs[WKBGeography](2).getWKBBytes.sameElements(geography.getWKBBytes))
      assert(row.isNullAt(3))
      val fromRows = StructuredAdapter.toSpatialRdd(source.rdd, "shape")
      val restoredRow = StructuredAdapter.toRowRdd(fromRows).first()
      assert(restoredRow.schema == schema)
      assert(restoredRow.getAs[Geometry]("shape").equalsExact(geometry))
    }

    it("retains null internal geometry as an empty carrier and restores the original null") {
      val schema =
        StructType(Seq(StructField("geometry", GeometryUDT()), StructField("id", IntegerType)))
      val source = sparkSession.createDataFrame(sc.parallelize(Seq(Row(null, 9))), schema)
      val spatialRDD = StructuredAdapter.toSpatialRdd(source)
      assert(spatialRDD.rawSpatialRDD.first().isEmpty)
      val restored = StructuredAdapter.toDf(spatialRDD, sparkSession).head()
      assert(restored.isNullAt(0))
      assert(restored.getInt(1) == 9)
    }

    it("detects geometry columns without selecting geography or hiding ambiguity") {
      def schema(names: String*): StructType = StructType(
        Seq(StructField("globe", GeographyUDT())) ++ names.map(StructField(_, GeometryUDT())))
      assert(DfUtils.getGeometryColumnName(schema("shape")) == "shape")
      assert(DfUtils.getGeometryColumnName(schema("left", "geometry")) == "geometry")
      intercept[IllegalArgumentException] { DfUtils.getGeometryColumnName(schema()) }
      intercept[IllegalArgumentException] {
        DfUtils.getGeometryColumnName(schema("left", "right"))
      }
    }
  }
}
