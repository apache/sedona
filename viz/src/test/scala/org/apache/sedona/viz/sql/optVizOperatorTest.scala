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

import org.apache.sedona.viz.sql.operator.{AggregateWithinPartitons, VizPartitioner}
import org.apache.sedona.viz.sql.utils.{Conf, LineageDecoder}
import org.locationtech.jts.geom.Envelope
import org.apache.spark.sql.functions.lit

class optVizOperatorTest extends TestBaseScala {

  describe("SedonaViz SQL function Test") {

    it("Passed full pipeline using optimized operator") {
      val table = spark.sql(
       """
         |SELECT pixel, shape FROM pointtable
         |LATERAL VIEW EXPLODE(ST_Pixelize(shape, 1000, 1000, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000))) AS pixel
        """.stripMargin)

      // Test visualization partitioner
      val zoomLevel = 2
      val newDf = VizPartitioner(table, zoomLevel, "pixel", new Envelope(0, 1000, 0, 1000))
      newDf.createOrReplaceTempView("pixels")
      assert(newDf.select(Conf.PrimaryPID).distinct().count() <= Math.pow(4, zoomLevel))
      val secondaryPID = newDf.select(Conf.SecondaryPID).distinct().count()
      assert(newDf.rdd.getNumPartitions == secondaryPID)

      // Test aggregation within partitions
      val result = AggregateWithinPartitons(newDf.withColumn("weight", lit(100.0)), "pixel", "weight", "avg")
      assert(result.rdd.getNumPartitions == secondaryPID)

      // Test the colorize operator
      result.createOrReplaceTempView("pixelaggregates")
      val colorTable = spark.sql(
        s"""
          |SELECT pixel, ${Conf.PrimaryPID}, ${Conf.SecondaryPID}, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates))
          |FROM pixelaggregates
        """.stripMargin)

      colorTable.show(1)
    }

    it("Passed full pipeline - aggregate:avg - color:uniform") {
      var table = spark.sql(
        """
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW EXPLODE(ST_Pixelize(shape, 1000, 1000, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000))) AS pixel
        """.stripMargin)

      // Test visualization partitioner
      val zoomLevel = 2
      val newDf = VizPartitioner(table, zoomLevel, "pixel", new Envelope(0, 1000, 0, 1000))
      newDf.createOrReplaceTempView("pixels")
      assert(newDf.select(Conf.PrimaryPID).distinct().count() <= Math.pow(4, zoomLevel))
      val secondaryPID = newDf.select(Conf.SecondaryPID).distinct().count()
      assert(newDf.rdd.getNumPartitions == secondaryPID)

      // Test aggregation within partitions
      val result = AggregateWithinPartitons(newDf, "pixel", "weight", "count")
      assert(result.rdd.getNumPartitions == secondaryPID)

      // Test the colorize operator
      result.createOrReplaceTempView("pixelaggregates")
      val colorTable = spark.sql(
        s"""
           |SELECT pixel, ${Conf.PrimaryPID}, ${Conf.SecondaryPID}, ST_Colorize(weight, 0, 'red')
           |FROM pixelaggregates
        """.stripMargin)
      colorTable.show(1)
    }

    it("Passed lineage decoder"){
      assert(LineageDecoder("01") == "2-1-0")
      assert(LineageDecoder("12") == "2-2-1")
      assert(LineageDecoder("333") == "3-7-7")
      assert(LineageDecoder("012") == "3-2-1")
    }
  }
}
