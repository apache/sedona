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

import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.BeforeAndAfterAll

import java.util.Collections
import scala.collection.JavaConverters._

case class GeoDataHex(id: Int, geometry_hex: String)
case class GeoData(id: Int, geometry: Geometry)

class GeoParquetMetadataTests extends TestBaseScala with BeforeAndAfterAll {
  val geoparquetdatalocation: String = resourceFolder + "geoparquet/"
  val geoparquetoutputlocation: String = resourceFolder + "geoparquet/geoparquet_output/"

  import sparkSession.implicits._

  describe("GeoParquet Metadata tests") {
    it("reading and writing GeoParquet files") {
//           'POINT(30.0123 10.2131)', \
      //                    'POINT(-20 20)', \
      //                    'POINT(10 30)', \
      //                    'POINT(40 -40)' \

//      [0] = {u8} 18
//[1] = {u8} 0
//[2] = {u8} 0
//[3] = {u8} 0
//[4] = {u8} 1
//[5] = {u8} 165
//[6] = {u8} 189
//[7] = {u8} 193
//[8] = {u8} 23
//[9] = {u8} 38
//[10] = {u8} 3
//[11] = {u8} 62
//[12] = {u8} 64
//[13] = {u8} 34
//[14] = {u8} 142
//[15] = {u8} 117
//[16] = {u8} 113
//[17] = {u8} 27
//[18] = {u8} 109
//[19] = {u8} 36
//[20] = {u8} 64
      val byteArray = Array[Int](
        18, 0, 0, 0, 1,
        165, 189, 193, 23, 38, 3, 62, 64, 34, 142, 117, 113, 27, 109, 36, 64
      )
        .map(_.toByte)

//      [ 18, 0, 0, 0, 1, -91, -67, -63, 23, 38, 3, 62, 64, 34, -114, 117, 113, 27, 109, 36, 64 ]

//      GeometrySerializer.deserialize(byteArray)
//        [18, 0, 0, 0, 1, -91, -67, -63, 23, 38, 3, 62, 64, 34, -114, 117, 113, 27, 109, 36, 64]
//    18 18
//    0 0
//    0 0
//    0 0
//    1 1
//    0
//    0
//    0
//    -91 -91
//    -67 -67
//    -63 -63
//    23 23
//    38 38
//    3 3
//    62 62
//    64 64
//    34 34
//    -114 -114
//    117 117
//    113 113
//    27 27
//    109 109
//    36 36
//    64 64

//      val wktReader = new WKTReader()
//      val pointWKT = "POINT(30.0123 10.2131)"
//      val point = wktReader.read(pointWKT)
//      val serializedBytes = GeometrySerializer.serialize(point)
//      serializedBytes.foreach(
//        byte => println(byte)
//      )
//
      def hexToBytes(hex: String): Array[Byte] =
        hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
//
      def bytesToHex(bytes: Array[Byte]): String =
        bytes.map("%02x".format(_)).mkString

//      Seq(
//        (1, "POINT(30.0123 10.2131)"),
////        (2, "POINT(-20 20)"),
////        (3, "POINT(10 30)"),
////        (4, "POINT(40 -40)")
//      ).toDF("id", "wkt")
//        .selectExpr("id", "ST_GeomFromWKT(wkt) AS geometry")
//        .as[GeoData]
//        .map(
//          row => (row.id, bytesToHex(GeometrySerializer.serialize(row.geometry)))
//        ).show(4, false)


//
//      val data = Seq(
//        (1, "1200000001000000a5bdc11726033e40228e75711b6d2440"),
//      )
//        .toDF("id", "geometry_hex")
//        .as[GeoDataHex]
//
//      data.map(
//        row => GeoData(row.id, GeometrySerializer.deserialize(hexToBytes(row.geometry_hex)))
//      ).show

      val wkt = "LINESTRING (  20.9972017 52.1696936,   20.9971687 52.1696659,   20.997156 52.169644,   20.9971487 52.1696213 ) "
      val reader = new WKTReader()
      val geometry = reader.read(wkt)
      val serialized = GeometrySerializer.serialize(geometry)

      Seq(
        (1, serialized)
      ).toDF("id", "geometry_bytes")
        .show(1, false)

//      println(bytesToHex(serialized))

//
//      val binaryData = "1200000001000000bb9d61f7b6c92c40f1ba168a85"
//      val binaryData2 = "120000000100000046b6f3fdd4083e404e62105839342440"
//      val value = new GeometryUDT().deserialize(hexToBytes(binaryData))
//      val value3 = new GeometryUDT().deserialize(hexToBytes(binaryData2))
//      println(value)
//      println(value3)
//
//      val reader = new WKTReader()
//      val geometryPoint = "POINT (30.0345 10.1020)"
//      val point = reader.read(geometryPoint)
//      val result = new GeometryUDT().serialize(point)
//
//      val value2 = new GeometryUDT().deserialize(result)
//      println(bytesToHex(result))
//      println(value2)
//      println("ssss")
    }
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
