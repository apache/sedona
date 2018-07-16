/*
 * FILE: predicateJoinTestScala.scala
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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait JoinTypeSpec extends SparkTestSpec {
  override def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession.sql(
      """
        |SELECT 1 AS point_id, ST_Point(75.0, -100.0) AS point
        |UNION SELECT 2 AS point_id, ST_Point(26.0, 15.0) AS point
      """.stripMargin)
      .createOrReplaceTempView("pointDf")

    sparkSession.sql(
      """
        |SELECT 3 AS polygon_id, ST_PolygonFromEnvelope(25.0, 10.0, 35.0, 20.0) AS polygon
        |UNION SELECT 4 AS polygon_id, ST_PolygonFromEnvelope(55.0, 10.0, 65.0, 20.0) AS polygon
      """.stripMargin)
      .createOrReplaceTempView("polygonDf")
  }

  def joinSql(joinType: String): DataFrame = {
    val joinedDf = sparkSession.sql(
      s"""
        |SELECT point_id,polygon_id FROM pointDf $joinType JOIN polygonDf
        |ON ST_Within(pointDf.point, polygonDf.polygon)
      """.stripMargin)
    joinedDf.explain()
    joinedDf
  }

  def leftJoin(): DataFrame = joinSql("LEFT OUTER")

  def rightJoin(): DataFrame = joinSql("RIGHT")

  def innerJoin(): DataFrame = joinSql("INNER")
}

class joinTypesWithLeftIndex extends JoinTypeSpec {
  override val sparkOptions = Map(
    "geospark.global.index" -> "true",
    "geospark.join.indexbuildside" -> "left"
  )

  describe("GeoSpark-SQL Join Types Test With Left Index") {
    it("Inner join") {
      val joinDf = innerJoin()
      assert(joinDf.collect().toSet sameElements Set(Row(2, 3)))
    }
    it("Left outer join") {
      val joinDf = leftJoin()
      intercept[IllegalArgumentException] {
        joinDf.collect()
      }
    }
    it("Right outer join") {
      val joinDf = rightJoin()
      assert(joinDf.collect().toSet sameElements Set(Row(null, 4), Row(2, 3)))
    }
  }
}

class joinTypesWithRightIndex extends JoinTypeSpec {
  override val sparkOptions = Map(
    "geospark.global.index" -> "true",
    "geospark.join.indexbuildside" -> "right"
  )

  describe("GeoSpark-SQL Join Types Test With Right Index") {
    it("Inner join") {
      val joinDf = innerJoin()
      assert(joinDf.collect().toSet sameElements Set(Row(2, 3)))
    }
    it("Left outer join") {
      val joinDf = leftJoin()
      assert(joinDf.collect().toSet sameElements Set(Row(1, null), Row(2, 3)))
    }
    it("Right outer join") {
      val joinDf = rightJoin()
      intercept[IllegalArgumentException] {
        joinDf.collect()
      }
    }
  }
}

class joinTypesWithNoIndex extends JoinTypeSpec {
  override val sparkOptions = Map(
    "geospark.global.index" -> "false"
  )

  describe("GeoSpark-SQL Join Types Test With NO Index") {
    it("Inner join") {
      val joinDf = innerJoin()
      assert(joinDf.collect().toSet sameElements Set(Row(2, 3)))
    }
    it("Left outer join") {
      val joinDf = leftJoin()
      assert(joinDf.collect().toSet sameElements Set(Row(1, null), Row(2, 3)))
    }
    it("Right outer join") {
      val joinDf = rightJoin()
      assert(joinDf.collect().toSet sameElements Set(Row(null, 4), Row(2, 3)))
    }
  }
}