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

import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{GridType, IndexType}
import org.apache.sedona.core.formatMapper.EarthdataHDFPointMapper
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.Point
import org.scalatest.GivenWhenThen

class adapterTestScala extends TestBaseScala with GivenWhenThen{

  describe("Sedona-SQL Scala Adapter Test") {

    it("Read CSV point into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_PointFromText(inputtable._c0,\",\") as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      val resultDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(resultDf.schema(0).dataType == GeometryUDT)
    }

    it("Read CSV point at a different column id into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select \'123\', \'456\', ST_PointFromText(inputtable._c0,\",\") as arealandmark, \'789\' from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, 2)
      spatialRDD.analyze()
      val newDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(newDf.schema.toList.map(f => f.name).mkString("\t").equals("geometry\t123\t456\t789"))
    }

    it("Read CSV point at a different column col name into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select \'123\', \'456\', ST_PointFromText(inputtable._c0,\",\") as arealandmark, \'789\' from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      spatialRDD.analyze()
      val newDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(newDf.schema.toList.map(f => f.name).mkString("\t").equals("geometry\t123\t456\t789"))
    }

    it("Read CSV point into a SpatialRDD by passing coordinates") {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "arealandmark")
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 1)
    }

    it("Read mixed WKT geometries into a SpatialRDD") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 1)
    }

    it("Read mixed WKT geometries into a SpatialRDD with uniqueId") {
      var df = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      df.createOrReplaceTempView("inputtable")
      var spatialDf = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty, inputtable._c3, inputtable._c5 from inputtable")
      var spatialRDD = Adapter.toSpatialRdd(spatialDf, "usacounty")
      spatialRDD.analyze()
      assert(Adapter.toDf(spatialRDD, sparkSession).columns.length == 3)
    }

    it("Read shapefile -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.schema.toList.map(f => f.name).mkString("\t").equals("geometry\tSTATEFP\tCOUNTYFP\tCOUNTYNS\tAFFGEOID\tGEOID\tNAME\tLSAD\tALAND\tAWATER"))
      assert(df.count() == 3220)
    }

    it("Read shapefileWithMissing -> DataFrame") {
      var spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileWithMissingsTrailingInputLocation)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.count() == 3)
    }

    it("Read GeoJSON to DataFrame") {
      var spatialRDD = new PolygonRDD(sparkSession.sparkContext, geojsonInputLocation, FileDataSplitter.GEOJSON, true)
      spatialRDD.analyze()
      var df = Adapter.toDf(spatialRDD, sparkSession)//.withColumn("geometry", callUDF("ST_GeomFromWKT", col("geometry")))
      assert(df.columns(1) == "STATEFP")
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
      assert(joinResultDf.schema(0).dataType == GeometryUDT)
      assert(joinResultDf.schema(1).dataType == GeometryUDT)
      assert(joinResultDf.schema(0).name == "leftgeometry")
      assert(joinResultDf.schema(1).name == "rightgeometry")
      import scala.jdk.CollectionConverters._
      val joinResultDf2 = Adapter.toDf(joinResultPairRDD, polygonRDD.fieldNames.asScala.toSeq, List(), sparkSession)
      assert(joinResultDf2.schema(0).dataType == GeometryUDT)
      assert(joinResultDf2.schema(0).name == "leftgeometry")
      assert(joinResultDf2.schema(1).name == "abc")
      assert(joinResultDf2.schema(2).name == "def")
      assert(joinResultDf2.schema(3).dataType == GeometryUDT)
      assert(joinResultDf2.schema(3).name == "rightgeometry")
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
      assert(joinResultDf.schema(0).dataType == GeometryUDT)
      assert(joinResultDf.schema(1).dataType == GeometryUDT)
      assert(joinResultDf.schema(0).name == "leftgeometry")
      assert(joinResultDf.schema(1).name == "rightgeometry")
    }

    it("load id column Data check") {
      var spatialRDD = new PolygonRDD(sparkSession.sparkContext, geojsonIdInputLocation, FileDataSplitter.GEOJSON, true)
      spatialRDD.analyze()
      val df = Adapter.toDf(spatialRDD, sparkSession)
      assert(df.columns.length == 4)
      assert(df.count() == 1)
    }

    // Tests here have been ignored. A new feature that reads HDF will be added.
    ignore("load HDF data from RDD to a DataFrame") {
      val InputLocation = "file://" + resourceFolder + "modis/modis.csv"
      val numPartitions = 5
      val HDFincrement = 5
      val HDFoffset = 2
      val HDFrootGroupName = "MOD_Swath_LST"
      val HDFDataVariableName = "LST"
      val urlPrefix = resourceFolder + "modis/"
      val HDFDataVariableList:Array[String] = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")
      val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFincrement, HDFoffset, HDFrootGroupName, HDFDataVariableList, HDFDataVariableName, urlPrefix)
      val spatialRDD = new PointRDD(sparkSession.sparkContext, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
      import scala.jdk.CollectionConverters._
      spatialRDD.fieldNames = HDFDataVariableList.dropRight(4).toList.asJava
      val spatialDf = Adapter.toDf(spatialRDD, sparkSession)
      assert(spatialDf.schema.fields(1).name == "LST")
    }

    it("can convert spatial RDD with user data to a valid Dataframe") {
      val srcDF = sparkSession.sql("select ST_PointFromText('40.7128,-74.0060', ',') as geom, \"attr1\" as attr1, \"attr2\" as attr2")
      val rdd = Adapter.toSpatialRdd(srcDF, "geom")
      val df = Adapter.toDf(rdd, Seq("attr1", "attr2"), sparkSession)
      df.unpersist(true)
      // verify the resulting Spark dataframe can be successfully evaluated repeatedly
      for (_ <- 1 to 5) {
        val rows = df.collect
        assert(rows.length == 1)
        val geom = rows(0).get(0).asInstanceOf[Point]
        assert(geom.getX == 40.7128)
        assert(geom.getY == -74.006)
        assert(rows(0).get(1).asInstanceOf[String] == "attr1")
        assert(rows(0).get(2).asInstanceOf[String] == "attr2")
      }
    }

    it("can convert spatial RDD to DataFrame with user-supplied schema") {
      val srcDF = sparkSession.sql("""
        select
          ST_PointFromText('40.7128,-74.0060', ',') as geom,
          'abc' as exampletext,
          1.23 as exampledouble,
          234 as exampleint

        union

        select
          ST_PointFromText('40.7128,-74.0060', ',') as geom,
          null as exampletext,
          null as exampledouble,
          null as exampleint
      """)

      // Convert to DataFrame
      // Tweak the column names to camelCase to ensure it also renames
      val schema = StructType(Array(
        StructField("leftGeometry", GeometryUDT, nullable = true),
        StructField("exampleText", StringType, nullable = true),
        StructField("exampleDouble", DoubleType, nullable = true),
        StructField("exampleInt", IntegerType, nullable = true)
      ))
      val rdd = Adapter.toSpatialRdd(srcDF, "geom")
      val df = Adapter.toDf(rdd, schema, sparkSession)

      // Check results
      // Force an action so that spark has to serialize the data -- this will surface
      // a serialization error if the schema or coercion is incorrect, e.g.
      // "Error while encoding: java.lang.RuntimeException: <desired data type> is not a
      // valid external type for schema of <current data type>"
      println(df.show(1))

      assert(
        df.schema == schema,
        s"Expected schema\n$schema\nbut got\n${df.schema}!"
      )
    }

    it("can convert JavaPairRDD to DataFrame with user-supplied schema") {
      // Prepare JavaPairRDD
      // Left table
      val pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      val pointDf = sparkSession.sql("""
        select
          ST_Point(
            cast(pointtable._c0 as Decimal(24,20)),
            cast(pointtable._c1 as Decimal(24,20))
          ) as arealandmark
        from pointtable
      """)
      val pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      // Right table
      val polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql("""
        select
          ST_GeomFromWKT(polygontable._c0) as usacounty,
          'abc' as exampletext,
          1.23 as examplefloat,
          1.23 as exampledouble,
          10 as exampleshort,
          234 as exampleint,
          9223372036854775800 as examplelong,
          true as examplebool,
          date('2022-01-01') as exampledate,
          timestamp('2022-01-01T00:00:00.000000Z') as exampletimestamp,
          named_struct('structtext', 'spark', 'structint', 5, 'structbool', false) as examplestruct
        from polygontable

        union

        -- Test that nulls can be properly encoded
        select
          ST_GeomFromWKT(polygontable._c0) as usacounty,
          null as exampletext,
          null as examplefloat,
          null as exampledouble,
          null as exampleshort,
          null as exampleint,
          null as examplelong,
          null as examplebool,
          null as exampledate,
          null as exampletimestamp,
          null as examplestruct
        from polygontable
      """)
      polygonDf.show(1)
      val polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      polygonRDD.spatialPartitioning(pointRDD.getPartitioner)
      pointRDD.buildIndex(IndexType.QUADTREE, true)
      val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD, polygonRDD, true, true)

      // Convert to DataFrame
      // Tweak the column names to camelCase to ensure it also renames
      val schema = StructType(Array(
        StructField("leftGeometry", GeometryUDT, nullable = true),
        StructField("exampleText", StringType, nullable = true),
        StructField("exampleFloat", FloatType, nullable = true),
        StructField("exampleDouble", DoubleType, nullable = true),
        StructField("exampleShort", ShortType, nullable = true),
        StructField("exampleInt", IntegerType, nullable = true),
        StructField("exampleLong", LongType, nullable = true),
        StructField("exampleBool", BooleanType, nullable = true),
        StructField("exampleDate", DateType, nullable = true),
        StructField("exampleTimestamp", TimestampType, nullable = true),
        StructField("exampleStruct", StructType(Array(
          StructField("structText", StringType, nullable = true),
          StructField("structInt", IntegerType, nullable = true),
          StructField("structBool", BooleanType, nullable = true)
        ))),
        StructField("rightGeometry", GeometryUDT, nullable = true),
        // We have to include a column for right user data (even though there is none)
        // since there is no way to distinguish between no data and nullable data
        StructField("rightUserData", StringType, nullable = true)
      ))
      val joinResultDf = Adapter.toDf(joinResultPairRDD, schema, sparkSession)

      // Check results
      // Force an action so that spark has to serialize the data -- this will surface
      // a serialization error if the schema or coercion is incorrect, e.g.
      // "Error while encoding: java.lang.RuntimeException: <desired data type> is not a
      // valid external type for schema of <current data type>"
      joinResultDf.foreach(r => ())

      assert(
        joinResultDf.schema == schema,
        s"Expected schema\n$schema\nbut got\n${joinResultDf.schema}!"
      )
    }

    /**
     * If providing fieldNames instead of a schema, it should return the same result
     * as if we do provide a schema but declare all non-geom fields to be StringType
     */
    it("is consistent with toDf without schema vs with all StringType fields") {
      // Prepare JavaPairRDD
      // Left table
      val pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(arealmPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      val pointDf = sparkSession.sql(
        """
        select
          ST_Point(
            cast(pointtable._c0 as Decimal(24,20)),
            cast(pointtable._c1 as Decimal(24,20))
          ) as arealandmark,
          null as userdata
        from pointtable
      """)
      val pointRDD = Adapter.toSpatialRdd(pointDf, "arealandmark")
      pointRDD.analyze()

      // Right table
      val polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      val polygonDf = sparkSession.sql(
        """
        select
          ST_GeomFromWKT(polygontable._c0) as usacounty,
          'abc' as exampletext,
          1.23 as exampledouble,
          234 as exampleint
        from polygontable
      """)
      val polygonRDD = Adapter.toSpatialRdd(polygonDf, "usacounty")
      polygonRDD.analyze()

      pointRDD.spatialPartitioning(GridType.QUADTREE)
      polygonRDD.spatialPartitioning(pointRDD.getPartitioner)
      pointRDD.buildIndex(IndexType.QUADTREE, true)
      val joinResultPairRDD = JoinQuery.SpatialJoinQueryFlat(pointRDD, polygonRDD, true, true)

      // Convert to DataFrame
      val schema = StructType(Array(
        StructField("leftgeometry", GeometryUDT, nullable = true),
        StructField("exampletext", StringType, nullable = true),
        StructField("exampledouble", StringType, nullable = true),
        StructField("exampleint", StringType, nullable = true),
        StructField("rightgeometry", GeometryUDT, nullable = true),
        StructField("userdata", StringType, nullable = true)
      ))
      val joinResultDf = Adapter.toDf(joinResultPairRDD, schema, sparkSession)
      val resultWithoutSchema = Adapter.toDf(joinResultPairRDD, Seq("exampletext", "exampledouble", "exampleint"), Seq("userdata"), sparkSession)

      // Check results
      // Force an action so that spark has to serialize the data -- this will surface
      // a serialization error if the schema or coercion is incorrect, e.g.
      // "Error while encoding: java.lang.RuntimeException: <desired data type> is not a
      // valid external type for schema of <current data type>"
      println(joinResultDf.show(1))

      assert(
        joinResultDf.schema == resultWithoutSchema.schema,
        s"Schema of\n$schema\ndoes not match result from supplying field names\n${joinResultDf.schema}!"
      )
    }

    it("can convert spatial pair RDD with user data to a valid Dataframe") {
      var srcDF = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      srcDF.createOrReplaceTempView("inputtable")
      var leftDF = sparkSession.sql("select ST_GeomFromWKT(inputtable._c0) as leftGeom, \"attr1\" as attr1, \"attr2\" as attr2 from inputtable")
      val rightDF = sparkSession.sql("select ST_PointFromText('40.7128,-74.0060', ',') as rightGeom, \"attr3\" as attr3, \"attr4\" as attr4")
      val leftRDD = Adapter.toSpatialRdd(leftDF, "leftGeom")
      leftRDD.analyze()
      val rightRDD = Adapter.toSpatialRdd(rightDF, "rightGeom")
      rightRDD.analyze()
      leftRDD.spatialPartitioning(GridType.QUADTREE)
      rightRDD.spatialPartitioning(leftRDD.getPartitioner)
      val pairRDD = JoinQuery.SpatialJoinQueryFlat(leftRDD, rightRDD, true, true)
      val pairDF = Adapter.toDf(pairRDD, Seq("attr1", "attr2"), Seq("attr3", "attr4"), sparkSession)
      pairDF.unpersist(true)
      // verify the resulting Spark dataframe can be successfully evaluated repeatedly
      for (_ <- 1 to 5) {
        val rows = pairDF.collect
        for (row <- rows) {
          assert(row.get(1).asInstanceOf[String] == "attr1")
          assert(row.get(2).asInstanceOf[String] == "attr2")
          val pt = row.get(3).asInstanceOf[Point]
          assert(pt.getX == 40.7128)
          assert(pt.getY == -74.006)
          assert(row.get(4).asInstanceOf[String] == "attr1")
          assert(row.get(5).asInstanceOf[String] == "attr2")
        }
      }
    }
  }
}
