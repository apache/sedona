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

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Polygon
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Point
import org.locationtech.jts.operation.overlayng.OverlayNGRobust
import org.scalatest.BeforeAndAfterAll

import java.util

class SpiderTests extends TestBaseScala with BeforeAndAfterAll {
  describe("Spider data generator tests") {
    it("generate data with default parameters") {
      val spiderDf = sparkSession.read.format("spider").load()
      assert(spiderDf.count() == 100)
      spiderDf.collect().foreach { row =>
        assert(row.getAs[Long]("id") >= 0)
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Point])
      }
    }

    it("generate spider data with specified records and partitions") {
      val spiderDf = sparkSession.read
        .format("spider")
        .option("N", "1000")
        .option("numPartitions", "10")
        .option("distribution", "uniform")
        .load()
      assert(spiderDf.count() == 1000)
      assert(spiderDf.rdd.getNumPartitions == 10)
      // ids should not be duplicated
      assert(spiderDf.select("id").distinct().count() == 1000)
    }

    it("generate data with 0 records") {
      var spiderDf = sparkSession.read
        .format("spider")
        .option("N", "0")
        .option("numPartitions", "10")
        .option("distribution", "uniform")
        .load()
      assert(spiderDf.count() == 0)
      assert(spiderDf.rdd.getNumPartitions == 0)

      spiderDf = sparkSession.read
        .format("spider")
        .option("N", "0")
        .option("numPartitions", "10")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.count() == 0)
      assert(spiderDf.rdd.getNumPartitions == 0)
    }

    it("generate data with small number of records") {
      var spiderDf = sparkSession.read
        .format("spider")
        .option("N", "10")
        .option("numPartitions", "10")
        .option("distribution", "uniform")
        .load()
      assert(spiderDf.count() == 10)
      assert(spiderDf.rdd.getNumPartitions == 10)

      spiderDf = sparkSession.read
        .format("spider")
        .option("N", "4")
        .option("numPartitions", "10")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.count() == 4)
      assert(spiderDf.rdd.getNumPartitions == 4)
    }

    it("generate data with specified seed") {
      val spiderDf1 = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "10")
        .option("distribution", "uniform")
        .option("seed", "12345")
        .load()
      val spiderDf2 = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "10")
        .option("distribution", "uniform")
        .option("seed", "12345")
        .load()
      assert(spiderDf1.count() == 100)
      assert(spiderDf2.count() == 100)
      val spiderDf1Data = spiderDf1.collect()
      val spiderDf2Data = spiderDf2.collect()
      assert(spiderDf1Data.length == spiderDf2Data.length)
      assert(spiderDf1Data.toSet == spiderDf2Data.toSet)

      val spiderDf3 = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "10")
        .option("distribution", "parcel")
        .option("seed", "23456")
        .load()
      assert(spiderDf3.count() == 100)
      val spiderDf3Data = spiderDf3.collect()
      assert(spiderDf1Data.toSet != spiderDf3Data.toSet)
    }

    it("generate data with specified geometry type") {
      val spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("geometryType", "polygon")
        .option("minSegments", "5")
        .option("maxSegments", "10")
        .load()
      assert(spiderDf.count() == 100)
      spiderDf.collect().foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Polygon])
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.getNumPoints >= 6 && geom.getNumPoints <= 11)
      }

      val spiderDf2 = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("geometryType", "box")
        .load()
      assert(spiderDf2.count() == 100)
      spiderDf2.collect().foreach { row =>
        assert(row.getAs[Geometry]("geometry").isInstanceOf[Polygon])
        val geom = row.getAs[Geometry]("geometry")
        assert(geom.getNumPoints == 5)
        assert(geom.isRectangle)
      }
    }

    it("generate data with specified transform") {
      val spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "10")
        .option("distribution", "uniform")
        .option("translateX", "10")
        .option("translateY", "20")
        .option("scaleX", "2")
        .option("scaleY", "2")
        .option("skewX", "0")
        .option("skewY", "0")
        .load()
      assert(spiderDf.count() == 100)
      spiderDf.collect().foreach { row =>
        val geom = row.getAs[Geometry]("geometry")
        // check if the geometry is transformed correctly
        // the transformed geometry should be a rectangle within region [10, 12] x [20, 22]
        val centroid = geom.getCentroid
        assert(centroid.getX >= 10 && centroid.getX <= 12)
        assert(centroid.getY >= 20 && centroid.getY <= 22)
      }
    }

    it("generate data with diagonal distribution") {
      val spiderDf = sparkSession.read
        .format("spider")
        .option("N", "1000")
        .option("numPartitions", "4")
        .option("percentage", "0.7")
        .option("distribution", "diagonal")
        .load()
      assert(spiderDf.rdd.getNumPartitions == 4)
      val records = spiderDf.collect()
      assert(records.length == 1000)
      // verify the distribution of the data
      val numPoints = records.count { row =>
        val geom = row.getAs[Geometry]("geometry").getCentroid
        Math.abs(geom.getX - geom.getY) < 0.000001
      }
      assert(numPoints >= 650 && numPoints <= 750)
    }

    it("generate parcel data") {
      val spiderDf = sparkSession.read
        .format("spider")
        .option("N", "1000")
        .option("numPartitions", "4")
        .option("dither", "0.1")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.count() == 1000)
      // ids should not be duplicated
      assert(spiderDf.select("id").distinct().count() == 1000)
      // verify the distribution of the data
      val geometries: Array[Geometry] = spiderDf.collect().map { row =>
        row.getAs[Geometry]("geometry")
      }
      var totalArea = 0.0
      val bounds = new Envelope(0, 1, 0, 1)
      geometries.foreach { geom =>
        assert(geom.isInstanceOf[Polygon])
        assert(geom.getNumPoints == 5)
        assert(geom.isRectangle)
        totalArea += geom.getArea
        assert(bounds.covers(geom.getEnvelopeInternal))
      }
      assert(totalArea >= 0.7 && totalArea <= 1.0)
      val unionArea = OverlayNGRobust.union(util.Arrays.asList(geometries: _*)).getArea
      assert(Math.abs(totalArea - unionArea) < 0.001)
    }

    it("generate parcel data with specified number of partitions") {
      var spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "1")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.rdd.getNumPartitions == 1)
      spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "3")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.rdd.getNumPartitions == 1)

      spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "4")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.rdd.getNumPartitions == 4)
      spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "15")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.rdd.getNumPartitions == 4)

      spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "16")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.rdd.getNumPartitions == 16)
      spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "63")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.rdd.getNumPartitions == 16)
      spiderDf = sparkSession.read
        .format("spider")
        .option("N", "100")
        .option("numPartitions", "64")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.rdd.getNumPartitions == 64)
    }

    it("generate parcel data with transformation") {
      val spiderDf = sparkSession.read
        .format("spider")
        .option("N", "1000")
        .option("numPartitions", "16")
        .option("dither", "0.1")
        .option("translateX", "10")
        .option("translateY", "20")
        .option("scaleX", "2")
        .option("scaleY", "2")
        .option("skewX", "0")
        .option("skewY", "0")
        .option("distribution", "parcel")
        .load()
      assert(spiderDf.count() == 1000)
      // verify the distribution of the data
      val geometries: Array[Geometry] = spiderDf.collect().map { row =>
        row.getAs[Geometry]("geometry")
      }
      var totalArea = 0.0
      val bounds = new Envelope(10, 12, 20, 22)
      geometries.foreach { geom =>
        assert(geom.isInstanceOf[Polygon])
        assert(geom.getNumPoints == 5)
        assert(geom.isRectangle)
        totalArea += geom.getArea
        assert(bounds.covers(geom.getEnvelopeInternal))
      }
      assert(totalArea >= 3.0 && totalArea <= 4.0)
      val unionArea = OverlayNGRobust.union(util.Arrays.asList(geometries: _*)).getArea
      assert(Math.abs(totalArea - unionArea) < 0.001)
    }
  }
}
