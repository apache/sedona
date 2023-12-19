/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Main.resourceFolder
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}


object SqlExample {

  val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
  val csvPointInputLocation = resourceFolder + "testpoint.csv"
  val shapefileInputLocation = resourceFolder + "shapefiles/dbf"
  val rasterdatalocation = resourceFolder + "raster/"

  def testPredicatePushdownAndRangeJonQuery(sedona: SparkSession):Unit =
  {
    val sedonaConf = new SedonaConf(sedona.conf)
    println(sedonaConf)

    var polygonCsvDf = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPolygonInputLocation)
    polygonCsvDf.createOrReplaceTempView("polygontable")
    polygonCsvDf.show()
    var polygonDf = sedona.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
    polygonDf.createOrReplaceTempView("polygondf")
    polygonDf.show()

    var pointCsvDF = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    pointCsvDF.show()
    var pointDf = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    pointDf.show()

    var rangeJoinDf = sedona.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
      "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

    // Write result to GeoParquet file
    rangeJoinDf.write.format("geoparquet").mode("overwrite").save("target/test-classes/output/join.parquet")
    rangeJoinDf.explain()
    rangeJoinDf.show(3)
    assert (rangeJoinDf.count()==500)
  }

  def testDistanceJoinQuery(sedona: SparkSession): Unit =
  {
    val sedonaConf = new SedonaConf(sedona.conf)
    println(sedonaConf)

    var pointCsvDF1 = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF1.createOrReplaceTempView("pointtable")
    pointCsvDF1.show()
    var pointDf1 = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
    pointDf1.createOrReplaceTempView("pointdf1")
    pointDf1.show()

    var pointCsvDF2 = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF2.createOrReplaceTempView("pointtable")
    pointCsvDF2.show()
    var pointDf2 = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
    pointDf2.createOrReplaceTempView("pointdf2")
    pointDf2.show()

    var distanceJoinDf = sedona.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")
    distanceJoinDf.explain()
    distanceJoinDf.show(10)
    assert (distanceJoinDf.count()==2998)
  }

  def testAggregateFunction(sedona: SparkSession): Unit =
  {
    val sedonaConf = new SedonaConf(sedona.conf)
    println(sedonaConf)

    var pointCsvDF = sedona.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    var pointDf = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    var boundary = sedona.sql("select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf")
    val coordinates:Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(1.1,101.1)
    coordinates(1) = new Coordinate(1.1,1100.1)
    coordinates(2) = new Coordinate(1000.1,1100.1)
    coordinates(3) = new Coordinate(1000.1,101.1)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    geometryFactory.createPolygon(coordinates)
    assert(boundary.take(1)(0).get(0)==geometryFactory.createPolygon(coordinates))
  }

  def testShapefileConstructor(sedona: SparkSession): Unit =
  {
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD = ShapefileReader.readToGeometryRDD(sedona.sparkContext, shapefileInputLocation)
    var rawSpatialDf = Adapter.toDf(spatialRDD,sedona)
    rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
    var spatialDf = sedona.sql("""
                                       | SELECT geometry, STATEFP, COUNTYFP
                                       | FROM rawSpatialDf
                                     """.stripMargin)
    spatialDf.show()
    spatialDf.printSchema()
  }

  def testRasterIOAndMapAlgebra(sedona: SparkSession): Unit = {
    var df = sedona.read.format("binaryFile").option("dropInvalid", true).load(rasterdatalocation).selectExpr("RS_FromGeoTiff(content) as raster", "path")
    df.printSchema()
    df.show()
    df.selectExpr("RS_Metadata(raster) as metadata", "RS_GeoReference(raster) as georef", "RS_NumBands(raster) as numBands").show(false)
    df.selectExpr("RS_AddBand(raster, raster, 1) as raster_extraband").show()
  }
}
