/*
 * FILE: adapterTestScala.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geosparksql

import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PolygonRDD}
import org.datasyslab.geosparksql.utils.Adapter

class adapterTestScala extends TestBaseScala {

  describe("GeoSpark-SQL Scala Adapter Test") {

    it("Read CSV point into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_PointFromText(inputtable._c0,\",\") as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      Adapter.toDf(spatialRDD, sparkSession).show(1)
    }

    it("Read CSV point at a different column id into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select \'123\', \'456\', ST_PointFromText(inputtable._c0,\",\") as arealandmark, \'789\' from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, 2)
      spatialRDD.analyze()
      val newDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(newDf.schema.toList.map(f=>f.name).mkString("\t").equals("geometry\t123\t456\t789"))
    }

    it("Read CSV point at a different column col name into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select \'123\', \'456\', ST_PointFromText(inputtable._c0,\",\") as arealandmark, \'789\' from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      val newDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(newDf.schema.toList.map(f=>f.name).mkString("\t").equals("geometry\t123\t456\t789"))
    }

    it("Read CSV point into a SpatialRDD by passing coordinates") {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length==1)
//      Adapter.toDf(spatialRDD, sparkSession).show(1)
    }

    it("Read CSV point into a SpatialRDD with unique Id by passing coordinates") {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      // Use Column _c0 as the unique Id but the id can be anything in the same row
      var spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length==1)
//      Adapter.toDf(spatialRDD, sparkSession).show(1)
    }


    it("Read mixed WKT geometries into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length==1)
    }

    it("Read mixed WKT geometries into a SpatialRDD with uniqueId") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty, inputtable._c3, inputtable._c5 from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length==3)
    }

    it("Read shapefile -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      df.show(1)
    }

    it("Read shapefileWithMissing -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileWithMissingsTrailingInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      df.show(1)
    }

    it("Read GeoJSON to DataFrame") {
      import org.apache.spark.sql.functions.{callUDF, col}
      var spatialRDD = new PolygonRDD(sparkSession.sparkContext, geojsonInputLocation, FileDataSplitter.GEOJSON, true)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession).withColumn("geometry", callUDF("ST_GeomFromWKT", col("geometry")))
      assert (df.columns(1) == "STATEFP")
    }

    it("Convert spatial join result to DataFrame") {
      val polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as usacounty, 'abc' as abc, 'def' as def from polygontable")
      val polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()

      val pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      val pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      val pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      polygonRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD, polygonRDD, true, true)

      val joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
      joinResultDf.show(1)

      val joinResultDf2 = Adapter.toDf(joinResultPairRDD, List("abc","def"), List(), sparkSession)
      joinResultDf2.show(1)
    }

    it("Convert distance join result to DataFrame") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      var pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as usacounty from polygontable")
      var polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()
      var circleRDD = new CircleRDD(polygonRDD, 0.2)

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      circleRDD.spatialPartitioning(pointRDD.getPartitioner)

      pointRDD.buildIndex(IndexType.QUADTREE, true)

      var joinResultPairRDD = JoinQuery.DistanceJoinQueryFlat(pointRDD, circleRDD, true, true)

      var joinResultDf = Adapter.toDf(joinResultPairRDD, sparkSession)
      joinResultDf.show(1)
    }

    it("load id column Data check"){
      var spatialRDD = new PolygonRDD(sparkSession.sparkContext, geojsonIdInputLocation, FileDataSplitter.GEOJSON, true)
      spatialRDD.analyze()
      val df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.columns.length == 4)
      assert(df.count() == 1)

    }

  }
}
