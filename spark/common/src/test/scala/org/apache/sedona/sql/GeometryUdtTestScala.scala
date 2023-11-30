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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{DataType, StructType}
import org.junit.rules.TemporaryFolder
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.BeforeAndAfter

class GeometryUdtTestScala extends TestBaseScala with BeforeAndAfter {

  import sparkSession.implicits._

  var tempFolder: TemporaryFolder = new TemporaryFolder
  var dataFrame: DataFrame = _

  before {
    dataFrame = Seq(Tuple2(47.636, 9.389))
      .toDF("latitude", "longitude")
      .withColumn("point", expr("ST_Point(longitude, latitude)"))
  }

  describe("GeometryUDT Test") {
    it("Should write dataframe with geometry in Parquet format") {
      tempFolder.create()

      dataFrame.write.parquet(tempFolder.getRoot.getPath + "/parquet")

      val readDataFrame = sparkSession.read.parquet(tempFolder.getRoot.getPath + "/parquet")
      val row = readDataFrame.collect()(0)
      assert(row.getAs[Double]("latitude") == 47.636)
      assert(row.getAs[Double]("longitude") == 9.389)
      assert(row.getAs[Geometry]("point").equals(new WKTReader().read("POINT (9.389 47.636)")))
    }

    it("Should be able to render and parse JSON schema") {
      assert(DataType.fromJson(dataFrame.schema.json).asInstanceOf[StructType].equals(dataFrame.schema))
    }

    it("Case object and new instance should be equals") {
      assert(GeometryUDT.equals(new GeometryUDT))
    }
  }

  after {
    tempFolder.delete()
  }
}
