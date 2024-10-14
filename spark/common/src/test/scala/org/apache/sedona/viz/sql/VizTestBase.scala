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
package org.apache.sedona.viz.sql

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.sql.TestBaseScala
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.sql.DataFrame

trait VizTestBase extends TestBaseScala {
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.datasyslab").setLevel(Level.WARN)

  val polygonInputLocationWkt = resourceFolder + "county_small.tsv"
  val polygonInputLocation = resourceFolder + "primaryroads-polygon.csv"
  val pointInputLocation = resourceFolder + "arealm.csv"

  override def beforeAll(): Unit = {
    SedonaVizRegistrator.registerAll(sparkSession)
    getPoint().createOrReplaceTempView("pointtable")
    getPolygon().createOrReplaceTempView("usdata")
  }

  def getPoint(): DataFrame = {
    val pointDf = sparkSession.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "false")
      .load(pointInputLocation)
      .sample(false, 1)
    pointDf
      .selectExpr("ST_Point(cast(_c0 as Decimal(24,20)),cast(_c1 as Decimal(24,20))) as shape")
      .filter(
        "ST_Contains(ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000),shape)")
  }

  def getPolygon(): DataFrame = {
    val polygonDf = sparkSession.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(polygonInputLocationWkt)
    polygonDf
      .selectExpr("ST_GeomFromWKT(_c0) as shape", "_c1 as rate", "_c2", "_c3")
      .filter(
        "ST_Contains(ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000),shape)")
  }

}
