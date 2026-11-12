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

import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.types.BinaryType

import scala.util.Try

/**
 * GeoParquet 2.0 stores geometries in Parquet's GEOMETRY and GEOGRAPHY logical types. The
 * fixtures under `geoparquet/native` were written by other tools: `example-2.0.0.parquet` is the
 * example file of the GeoParquet 2.0.0 release, the `spark-4.2-*` files come from Spark 4.2's own
 * Parquet writer, and the `crs-*` files come from parquet-java 1.17.0, one per form the Parquet
 * format allows for the CRS of a column. Each `crs-*` file holds three POINT (1 2) rows.
 */
class GeoParquetNativeTypesSuite extends TestBaseScala {
  private val nativeFolder = resourceFolder + "geoparquet/native/"

  /** Parquet libraries older than 1.16.0 have no geospatial logical types. */
  private val libraryHasGeospatialTypes = Try(
    Class.forName(
      "org.apache.parquet.schema.LogicalTypeAnnotation$GeometryLogicalTypeAnnotation")).isSuccess

  private def read(file: String) =
    sparkSession.read.format("geoparquet").load(nativeFolder + file)

  private def messages(error: Throwable): String = Iterator
    .iterate(error)(_.getCause)
    .takeWhile(_ != null)
    .flatMap(cause => Option(cause.getMessage))
    .mkString("\n")

  describe("Reading Parquet native geospatial types") {
    if (libraryHasGeospatialTypes) {
      it("reads the GeoParquet 2.0.0 example file") {
        val df = read("example-2.0.0.parquet")
        assert(SpatialTypeSupport.isGeometry(df.schema("geometry").dataType))
        val rows = df.collect()
        assert(rows.length == 5)
        val fiji = rows.find(_.getAs[String]("name") == "Fiji").get
        assert(fiji.getJtsGeometry("geometry").getGeometryType == "MultiPolygon")
        assert(fiji.getJtsGeometry("geometry").getSRID == 4326)
      }

      // The CRS forms a GeoParquet 2.0 reader must or should understand, none of these files
      // has GeoParquet metadata.
      Seq(
        "crs-absent.parquet" -> 4326,
        "crs-authority-code.parquet" -> 3857,
        "crs-projjson.parquet" -> 32610,
        "crs-srid.parquet" -> 32610,
        "crs-srid-0.parquet" -> 0,
        "crs-projjson-key.parquet" -> 32610,
        "spark-4.2-geometry-3857.parquet" -> 3857,
        "spark-4.2-geometry-srid-0.parquet" -> 0).foreach { case (file, srid) =>
        it(s"reads $file without GeoParquet metadata as geometry with SRID $srid") {
          val df = read(file)
          assert(SpatialTypeSupport.isGeometry(df.schema("geometry").dataType))
          val geometries = df.collect().map(_.getJtsGeometry("geometry"))
          assert(geometries.length == 3)
          geometries.foreach { geometry =>
            assert(geometry.toText == "POINT (1 2)")
            assert(geometry.getSRID == srid)
          }
        }
      }

      Seq("crs-unresolvable.parquet", "crs-projjson-key-missing.parquet").foreach { file =>
        it(s"treats the CRS of $file as unknown, which native Spark types reject") {
          if (SpatialTypeSupport.usesNativeTypes) {
            val error = intercept[Exception](read(file).collect())
            assert(messages(error).contains("cannot be represented by a Spark native SRID"))
          } else {
            val geometries = read(file).collect().map(_.getJtsGeometry("geometry"))
            assert(geometries.map(_.getSRID).toSeq == Seq(0, 0, 0))
          }
        }
      }

      it("applies a spatial filter to a file without GeoParquet metadata") {
        val df = read("crs-srid-0.parquet")
        def count(envelope: String): Long =
          df.where(s"ST_Intersects(geometry, ST_GeomFromText('$envelope'))").count()
        assert(count("POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))") == 3)
        assert(count("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))") == 0)
      }

      it("reads a GEOGRAPHY column as WKB") {
        val df = read("spark-4.2-geography.parquet")
        assert(df.schema("geography").dataType == BinaryType)
        val wkb = df.selectExpr("hex(geography)").collect().map(_.getString(0))
        assert(wkb.toSeq == Seq.fill(3)("0101000000000000000000F03F0000000000000040"))
      }

      it("reports a file with neither native types nor GeoParquet metadata") {
        val error = intercept[Exception] {
          sparkSession.read
            .format("geoparquet")
            .load(resourceFolder + "geoparquet/plain.parquet")
            .collect()
        }
        assert(messages(error).contains("does not contain valid geo metadata"))
      }
    }
  }
}
