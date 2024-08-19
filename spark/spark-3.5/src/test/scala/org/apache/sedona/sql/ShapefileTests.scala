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

import org.scalatest.BeforeAndAfterAll

class ShapefileTests extends TestBaseScala with BeforeAndAfterAll {
  describe("Shapefile read tests") {
    it("read shapefile") {
      val shapefileDf = sparkSession.read
        .format("shapefile")
        .load(resourceFolder + "shapefiles/gis_osm_pois_free_1")
      shapefileDf.printSchema()
      shapefileDf.show(10, false)
      println(shapefileDf.count)

      shapefileDf.select("geometry", "code").show(10, false)
      shapefileDf.select("code", "osm_id").show(10, false)
      shapefileDf.selectExpr("geometry AS geom", "code", "osm_id as id").show(10, false)
      shapefileDf
        .selectExpr("ST_Centroid(geometry) AS centroid", "code", "osm_id as id")
        .show(10, false)
    }
  }
}
