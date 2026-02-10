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

import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.junit.rules.TemporaryFolder
import org.scalatest.BeforeAndAfter

class RasterUDTSuite extends TestBaseScala with BeforeAndAfter {

  var tempFolder: TemporaryFolder = new TemporaryFolder

  describe("RasterUDT Test") {
    it("Case object and new instance should be equals") {
      assert(RasterUDT == RasterUDT)
      val udt = new RasterUDT
      assert(udt.equals(udt))
      assert(udt.equals(RasterUDT))
      assert(RasterUDT.equals(udt))
    }

    it("hashCode should work correctly") {
      val udt = new RasterUDT
      assert(udt.hashCode() == RasterUDT.hashCode())
    }

    it("Should be able to render and parse JSON schema with RasterUDT") {
      // This reproduces the Delta write bug (#2608):
      // Delta and Parquet serialize the schema to JSON, then deserialize it.
      // Without a jsonValue override, the class name includes a '$' suffix
      // from the Scala case object, causing ClassNotFoundException on read.
      val rasterDf = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(1, 10, 10, 0, 0, 1) as raster")
      assert(
        DataType
          .fromJson(rasterDf.schema.json)
          .asInstanceOf[StructType]
          .equals(rasterDf.schema))
    }

    it("Should write and read raster DataFrame in Parquet format") {
      // Parquet also serializes schema as JSON, triggering the same bug as Delta.
      tempFolder.create()
      val rasterDf = sparkSession.sql(
        "SELECT RS_MakeEmptyRaster(1, 10, 10, 0, 0, 1) as raster")

      rasterDf.write.parquet(tempFolder.getRoot.getPath + "/raster_parquet")

      val readDf =
        sparkSession.read.parquet(tempFolder.getRoot.getPath + "/raster_parquet")
      assert(readDf.schema.fields(0).dataType.isInstanceOf[RasterUDT])
      assert(readDf.count() == 1)
    }

    it("RS_Union_Aggr output should write and read Parquet successfully") {
      // Users reported (#2608) that RS_Union_Aggr output can be written to Delta/Parquet
      // while RS_MakeEmptyRaster output cannot. RS_Union_Aggr uses ExpressionEncoder
      // which resolves the UDT via UDTRegistration (class name without '$' suffix),
      // whereas InferredExpression-based functions use the case object singleton
      // whose getClass.getName includes '$'.
      tempFolder.create()
      val rasterDf = sparkSession.sql(
        """SELECT RS_Union_Aggr(raster) as raster FROM (
          |  SELECT RS_MakeEmptyRaster(1, 10, 10, 0, 0, 1) as raster
          |)""".stripMargin)

      rasterDf.write.parquet(tempFolder.getRoot.getPath + "/union_aggr_parquet")

      val readDf =
        sparkSession.read.parquet(tempFolder.getRoot.getPath + "/union_aggr_parquet")
      assert(readDf.schema.fields(0).dataType.isInstanceOf[RasterUDT])
      assert(readDf.count() == 1)
    }
  }

  after {
    tempFolder.delete()
  }
}
