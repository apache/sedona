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
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll

import java.util.Collections
import scala.collection.JavaConverters._

class GeoParquetMetadataTests extends TestBaseScala with BeforeAndAfterAll {
  val geoparquetdatalocation: String = resourceFolder + "geoparquet/"
  val geoparquetoutputlocation: String = resourceFolder + "geoparquet/geoparquet_output/"

  describe("GeoParquet Metadata tests") {
    it("Reading GeoParquet Metadata") {
      val df = sparkSession.read.format("geoparquet.metadata").load(geoparquetdatalocation)
      val metadataArray = df.collect()
      assert(metadataArray.length > 1)
      assert(metadataArray.exists(_.getAs[String]("path").endsWith(".parquet")))
      assert(metadataArray.exists(_.getAs[String]("version") == "1.0.0-dev"))
      assert(metadataArray.exists(_.getAs[String]("primary_column") == "geometry"))
      assert(metadataArray.exists { row =>
        val columnsMap = row.getJavaMap(row.fieldIndex("columns"))
        columnsMap != null && columnsMap
          .containsKey("geometry") && columnsMap.get("geometry").isInstanceOf[Row]
      })
      assert(metadataArray.forall { row =>
        val columnsMap = row.getJavaMap(row.fieldIndex("columns"))
        if (columnsMap == null || !columnsMap.containsKey("geometry")) true
        else {
          val columnMetadata = columnsMap.get("geometry").asInstanceOf[Row]
          columnMetadata.getAs[String]("encoding") == "WKB" &&
          columnMetadata
            .getList[Any](columnMetadata.fieldIndex("bbox"))
            .asScala
            .forall(_.isInstanceOf[Double]) &&
          columnMetadata
            .getList[Any](columnMetadata.fieldIndex("geometry_types"))
            .asScala
            .forall(_.isInstanceOf[String]) &&
          columnMetadata.getAs[String]("crs").nonEmpty &&
          columnMetadata.getAs[String]("crs") != "null"
        }
      })
    }

    it("Reading GeoParquet Metadata with column pruning") {
      val df = sparkSession.read.format("geoparquet.metadata").load(geoparquetdatalocation)
      val metadataArray = df
        .selectExpr("path", "substring(primary_column, 1, 2) AS partial_primary_column")
        .collect()
      assert(metadataArray.length > 1)
      assert(metadataArray.forall(_.length == 2))
      assert(metadataArray.exists(_.getAs[String]("path").endsWith(".parquet")))
      assert(metadataArray.exists(_.getAs[String]("partial_primary_column") == "ge"))
    }

    it("Reading GeoParquet Metadata of plain parquet files") {
      val df = sparkSession.read.format("geoparquet.metadata").load(geoparquetdatalocation)
      val metadataArray = df.where("path LIKE '%plain.parquet'").collect()
      assert(metadataArray.nonEmpty)
      assert(metadataArray.forall(_.getAs[String]("path").endsWith("plain.parquet")))
      assert(metadataArray.forall(_.getAs[String]("version") == null))
      assert(metadataArray.forall(_.getAs[String]("primary_column") == null))
      assert(metadataArray.forall(_.getAs[String]("columns") == null))
    }

    it("Read GeoParquet without CRS") {
      val df = sparkSession.read
        .format("geoparquet")
        .load(geoparquetdatalocation + "/example-1.0.0-beta.1.parquet")
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_crs_omit.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", "")
        .mode("overwrite")
        .save(geoParquetSavePath)
      val dfMeta = sparkSession.read.format("geoparquet.metadata").load(geoParquetSavePath)
      val row = dfMeta.collect()(0)
      val metadata = row.getJavaMap(row.fieldIndex("columns")).get("geometry").asInstanceOf[Row]
      assert(metadata.getAs[String]("crs") == "")
    }

    it("Read GeoParquet with null CRS") {
      val df = sparkSession.read
        .format("geoparquet")
        .load(geoparquetdatalocation + "/example-1.0.0-beta.1.parquet")
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_crs_null.parquet"
      df.write
        .format("geoparquet")
        .option("geoparquet.crs", "null")
        .mode("overwrite")
        .save(geoParquetSavePath)
      val dfMeta = sparkSession.read.format("geoparquet.metadata").load(geoParquetSavePath)
      val row = dfMeta.collect()(0)
      val metadata = row.getJavaMap(row.fieldIndex("columns")).get("geometry").asInstanceOf[Row]
      assert(metadata.getAs[String]("crs") == "null")
    }

    it("Read GeoParquet with snake_case geometry column name and camelCase column name") {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("geom_column_1", GeometryUDT, nullable = false),
          StructField("geomColumn2", GeometryUDT, nullable = false)))
      val df = sparkSession.createDataFrame(Collections.emptyList[Row](), schema)
      val geoParquetSavePath = geoparquetoutputlocation + "/gp_column_name_styles.parquet"
      df.write.format("geoparquet").mode("overwrite").save(geoParquetSavePath)

      val dfMeta = sparkSession.read.format("geoparquet.metadata").load(geoParquetSavePath)
      val row = dfMeta.collect()(0)
      val metadata = row.getJavaMap(row.fieldIndex("columns"))
      assert(metadata.containsKey("geom_column_1"))
      assert(!metadata.containsKey("geoColumn1"))
      assert(metadata.containsKey("geomColumn2"))
      assert(!metadata.containsKey("geom_column2"))
      assert(!metadata.containsKey("geom_column_2"))
    }

    it("Read GeoParquet with covering metadata") {
      val dfMeta = sparkSession.read
        .format("geoparquet.metadata")
        .load(geoparquetdatalocation + "/example-1.1.0.parquet")
      val row = dfMeta.collect()(0)
      val metadata = row.getJavaMap(row.fieldIndex("columns")).get("geometry").asInstanceOf[Row]
      val covering = metadata.getAs[String]("covering")
      assert(covering.nonEmpty)
      Seq("bbox", "xmin", "ymin", "xmax", "ymax").foreach { key =>
        assert(covering contains key)
      }
    }
  }
}
