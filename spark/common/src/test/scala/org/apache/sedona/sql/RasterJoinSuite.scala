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

import org.apache.sedona.common.raster.{RasterConstructors, RasterPredicates}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastIndexJoinExec, RangeJoinExec}
import org.geotools.coverage.grid.GridCoverage2D
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.scalatest.prop.TableDrivenPropertyChecks

class RasterJoinSuite extends TestBaseScala with TableDrivenPropertyChecks {

  private val spatialJoinPartitionSideConfKey = "sedona.join.spatitionside"

  private val rasters: Seq[(GridCoverage2D, Int)] = Seq(
    // Japan
    makeRaster(774, 787, 301485, 4106715, 300, 32654),
    // US
    makeRaster(751, 742, 332385, 4258815, 300, 32610),
    // China
    makeRaster(1098, 1098, 399960, 4100040, 100, 32649),
    // Europe
    makeRaster(100, 100, 5418900, 4440600, 1000, 3035),
    // Crossing the anti-meridian
    makeRaster(428, 419, 306210, 7840890, 600, 32601),
    // Covering the north pole
    makeRaster(256, 256, -345000.000, 345000.000, 2000, 3996),
    // Raster without CRS
    makeRaster(100, 100, 0, 10, 0.1, 0)).zipWithIndex

  private val geometries: Seq[(Geometry, Int)] = Seq(
    // Containing the raster in Japan
    makeGeometry(
      """Polygon ((15446517.35428962484002113 4497568.35138410236686468,
        |15363592.98878422006964684 4155302.09768042247742414,
        |15848131.43820795789361 4070751.76422393182292581,
        |15871707.97349870949983597 4495942.38343301601707935,
        |15446517.35428962484002113 4497568.35138410236686468))
        |""".stripMargin,
      3857),

    // Within the raster in US
    makeGeometry(
      """Polygon ((-123.82577135452658013 37.61258454574337406,
        |-123.92028577870047457 37.24286636670893103,
        |-123.19217465913880005 37.48769634283748786,
        |-123.82577135452658013 37.61258454574337406))
        |""".stripMargin,
      4326),

    // Intersects with the raster in China
    makeGeometry(
      """Polygon ((365263.47518765379209071 4063668.33383210469037294,
        |399451.69224443659186363 3941531.93014299822971225,
        |460336.44393422664143145 4039658.63395863398909569,
        |365263.47518765379209071 4063668.33383210469037294))
        |""".stripMargin,
      32649),
    makeGeometry(
      """Polygon ((365263.47518765379209071 4063668.33383210469037294,
        |399451.69224443659186363 3941531.93014299822971225,
        |460336.44393422664143145 4039658.63395863398909569,
        |365263.47518765379209071 4063668.33383210469037294))
        |""".stripMargin,
      32649),

    // Within the raster in Europe. EPSG:3035 is in northing/easting axis order.
    makeGeometry("POINT (5484765 4366950)", 3035),
    // The same point in WGS84 lon/lat order
    makeGeometry("POINT (31.69291306160833 60.778101471493095)", 0),

    // Intersects with the raster crossing the anti-meridian
    makeGeometry(
      """Polygon ((19881508.42966464906930923 11095342.82182723470032215,
        |19721217.12711662799119949 11011121.96794607117772102,
        |19704916.31668801605701447 10649787.33677849918603897,
        |20033649.32699836418032646 10611752.11244506947696209,
        |20020065.31830785423517227 10978520.34708884730935097,
        |19881508.42966464906930923 11095342.82182723470032215))""".stripMargin,
      3857),
    makeGeometry(
      """Polygon ((480448.66039875813294202 7820337.926456643268466,
        |439091.24477483704686165 7744293.64611588511615992,
        |463772.2831310480250977 7678255.19213575311005116,
        |549155.33474172384012491 7620888.45433483086526394,
        |659219.42470861051697284 7688261.01849637925624847,
        |582508.08927714405581355 7840349.57917789556086063,
        |480448.66039875813294202 7820337.926456643268466))""".stripMargin,
      32601),
    makeGeometry(
      """Polygon ((-179.79160719945980418 72.40167640805925942,
        |-179.88457658759955393 67.24187536630319073,
        |-177.60682657817568497 67.35808710147787792,
        |-179.79160719945980418 72.40167640805925942))
        |""".stripMargin,
      4326),
    // Geometry with SRID = 0 is treated as in WGS84
    makeGeometry(
      """Polygon ((-179.79160719945980418 72.40167640805925942,
        |-179.88457658759955393 67.24187536630319073,
        |-177.60682657817568497 67.35808710147787792,
        |-179.79160719945980418 72.40167640805925942))
        |""".stripMargin,
      0),

    // Contains the raster crossing the anti-meridian
    makeGeometry(
      """Polygon ((228229.14750097354408354 7980499.11379990447312593,
        |166496.77378122112713754 7523416.85732173826545477,
        |760178.32529883971437812 7453803.75504201743751764,
        |810089.60617863957304507 7954230.01860001031309366,
        |228229.14750097354408354 7980499.11379990447312593))
        |""".stripMargin,
      32601),

    // Within the raster crossing the anti-meridian
    makeGeometry(
      """Polygon ((400291.72106028313282877 7727823.2543459190055728,
        |396351.35678029892733321 7689733.06630607135593891,
        |438381.90910013020038605 7676598.51870612427592278,
        |400291.72106028313282877 7727823.2543459190055728))""".stripMargin,
      32601),

    // Within the raster covering the north pole, this geometry is also covering the north pole.
    makeGeometry(
      """Polygon ((-92518.3246073299087584 55224.08376963355112821,
        |-29188.48167539271526039 -131950.78534031414892524,
        |132654.45026178006082773 101665.96858638746198267,
        |-92518.3246073299087584 55224.08376963355112821))""".stripMargin,
      3996),

    // Intersects with the raster covering the north pole
    makeGeometry(
      """Polygon ((-362725.6544502618489787 202993.71727748692501336,
        |-160070.15706806292291731 378909.94764397910330445,
        |-438721.46596858644625172 416907.85340314143104479,
        |-362725.6544502618489787 202993.71727748692501336))""".stripMargin,
      3996),
    makeGeometry(
      """Polygon ((14135017.69104760140180588 21873587.49849328026175499,
        |13762089.41573194414377213 27976050.18547679483890533,
        |19796746.96174897253513336 24857013.7010185532271862,
        |14135017.69104760140180588 21873587.49849328026175499))""".stripMargin,
      3857),

    // Within the raster with no CRS
    makeGeometry("""Point (5 5)""", 0)).zipWithIndex

  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareTempViewsForTestData()
  }

  describe("Sedona-SQL Spatial Join Test") {
    val joinConditions = Table(
      "join condition",
      ("df1 JOIN df2", "RS_Intersects(df1.rast, df2.geom)"),
      ("df1 JOIN df2", "RS_Intersects(df2.geom, df1.rast)"),
      ("df2 JOIN df1", "RS_Intersects(df1.rast, df2.geom)"),
      ("df2 JOIN df1", "RS_Intersects(df2.geom, df1.rast)"),
      ("df1 JOIN df2", "RS_Contains(df1.rast, df2.geom)"),
      ("df1 JOIN df2", "RS_Contains(df2.geom, df1.rast)"),
      ("df2 JOIN df1", "RS_Contains(df1.rast, df2.geom)"),
      ("df2 JOIN df1", "RS_Contains(df2.geom, df1.rast)"),
      ("df1 JOIN df2", "RS_Within(df1.rast, df2.geom)"),
      ("df1 JOIN df2", "RS_Within(df2.geom, df1.rast)"),
      ("df2 JOIN df1", "RS_Within(df1.rast, df2.geom)"),
      ("df2 JOIN df1", "RS_Within(df2.geom, df1.rast)"))

    forAll(joinConditions) { case (joinClause, joinCondition) =>
      val expected = buildExpectedResult(joinCondition)
      it(s"$joinClause ON $joinCondition, with left side as dominant side") {
        withConf(Map(spatialJoinPartitionSideConfKey -> "left")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM $joinClause ON $joinCondition")
          verifyResult(expected, result)
        }
      }
      it(s"$joinClause ON $joinCondition, with right side as dominant side") {
        withConf(Map(spatialJoinPartitionSideConfKey -> "right")) {
          val result =
            sparkSession.sql(s"SELECT df1.id, df2.id FROM $joinClause ON $joinCondition")
          verifyResult(expected, result)
        }
      }
      it(s"$joinClause ON $joinCondition, broadcast df1") {
        val result = sparkSession.sql(
          s"SELECT /*+ BROADCAST(df1) */ df1.id, df2.id FROM $joinClause ON $joinCondition")
        verifyResult(expected, result)
      }
      it(s"$joinClause ON $joinCondition, broadcast df2") {
        val result = sparkSession.sql(
          s"SELECT /*+ BROADCAST(df2) */ df1.id, df2.id FROM $joinClause ON $joinCondition")
        verifyResult(expected, result)
      }
    }
  }

  describe("raster-raster join") {
    val expected = rasters.flatMap { case (rast, id1) =>
      rasters.flatMap { case (otherRast, id2) =>
        if (RasterPredicates.rsIntersects(rast, otherRast)) Some((id1, id2)) else None
      }
    }
    it("raster-raster join, with left side as dominant side") {
      withConf(Map(spatialJoinPartitionSideConfKey -> "left")) {
        val result = sparkSession.sql(
          "SELECT df1.id, df3.id FROM df1 JOIN df3 ON RS_Intersects(df1.rast, df3.rast)")
        verifyResult(expected, result)
      }
    }
    it("raster-raster join, with right side as dominant side") {
      withConf(Map(spatialJoinPartitionSideConfKey -> "right")) {
        val result = sparkSession.sql(
          "SELECT df1.id, df3.id FROM df1 JOIN df3 ON RS_Intersects(df1.rast, df3.rast)")
        verifyResult(expected, result)
      }
    }
    it("raster-raster join, broadcast left") {
      val result = sparkSession.sql(
        "SELECT /*+ BROADCAST(df1) */ df1.id, df3.id FROM df1 JOIN df3 ON RS_Intersects(df1.rast, df3.rast)")
      verifyResult(expected, result)
    }
    it("raster-raster join, broadcast right") {
      val result = sparkSession.sql(
        "SELECT /*+ BROADCAST(df3) */ df1.id, df3.id FROM df1 JOIN df3 ON RS_Intersects(df1.rast, df3.rast)")
      verifyResult(expected, result)
    }
  }

  private def prepareTempViewsForTestData(): Unit = {
    import sparkSession.implicits._
    rasters.toDF("rast", "id").createOrReplaceTempView("df1")
    geometries.toDF("geom", "id").createOrReplaceTempView("df2")
    rasters.toDF("rast", "id").createOrReplaceTempView("df3")
  }

  private def makeRaster(
      width: Int,
      height: Int,
      upperLeftX: Double,
      upperLeftY: Double,
      pixelSize: Double,
      srid: Int): GridCoverage2D =
    RasterConstructors.makeEmptyRaster(
      1,
      "B",
      width,
      height,
      upperLeftX,
      upperLeftY,
      pixelSize,
      -pixelSize,
      0,
      0,
      srid)

  private def makeGeometry(wkt: String, srid: Int): Geometry = {
    val wktReader = new WKTReader()
    val geom = wktReader.read(wkt)
    geom.setSRID(srid)
    geom
  }

  private def buildExpectedResult(joinCondition: String): Seq[(Int, Int)] = {
    val evaluate = joinCondition match {
      case "RS_Intersects(df1.rast, df2.geom)" | "RS_Intersects(df2.geom, df1.rast)" =>
        (r: GridCoverage2D, g: Geometry) => RasterPredicates.rsIntersects(r, g)
      case "RS_Contains(df1.rast, df2.geom)" | "RS_Within(df2.geom, df1.rast)" =>
        (r: GridCoverage2D, g: Geometry) => RasterPredicates.rsContains(r, g)
      case "RS_Contains(df2.geom, df1.rast)" | "RS_Within(df1.rast, df2.geom)" =>
        (r: GridCoverage2D, g: Geometry) => RasterPredicates.rsWithin(r, g)
    }
    rasters.flatMap { case (rast, rastId) =>
      geometries.flatMap { case (geom, geomId) =>
        if (evaluate(rast, geom)) {
          Some((rastId, geomId))
        } else {
          None
        }
      }
    }
  }

  private def verifyResult(expected: Seq[(Int, Int)], result: DataFrame): Unit = {
    isUsingOptimizedSpatialJoin(result)
    val actual = result.collect().map(row => (row.getInt(0), row.getInt(1))).sorted
    assert(actual.nonEmpty)
    assert(actual === expected)
  }

  private def isUsingOptimizedSpatialJoin(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collect { case _: BroadcastIndexJoinExec | _: RangeJoinExec =>
      true
    }.nonEmpty
  }
}
