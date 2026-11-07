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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.common.geography.{Constructors => GeographyConstructors}
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/**
 * Compare spherical SQL results with and without spatial planning, including globe boundaries.
 */
class LegacyGeographyDistanceJoinSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var session: SparkSession = _
  private var ownedContext = false
  private var previous: Option[SparkSession] = None

  override def beforeAll(): Unit = {
    super.beforeAll()
    previous = SparkSession.getActiveSession
    val borrowed = previous.orElse(SparkSession.getDefaultSession)
    ownedContext = borrowed.isEmpty
    session = borrowed.map(_.newSession()).getOrElse {
      SedonaContext
        .builder()
        .master("local[2]")
        .appName(getClass.getSimpleName)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    }
    SparkSession.setActiveSession(session)
    session.conf.set("spark.sql.adaptive.enabled", "false")
    session.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    session.conf.set("sedona.join.autoBroadcastJoinThreshold", "-1")
    session.conf.set("sedona.join.numpartition", "2")
    SedonaContext.create(session)
  }

  override def afterAll(): Unit = {
    try {
      if (session != null) {
        session.catalog.dropTempView("geog_distance_left")
        session.catalog.dropTempView("geog_distance_right")
        if (ownedContext) session.stop()
      }
      previous match {
        case Some(value) => SparkSession.setActiveSession(value)
        case None => SparkSession.clearActiveSession()
      }
    } finally super.afterAll()
  }

  for (srid <- Seq(4326, 4269); broadcast <- Seq(false, true)) {
    test(s"geography ST_Distance retains S2 answers for SRID $srid with broadcast=$broadcast") {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("g", GeographyUDT()),
          StructField("radius", DoubleType)))
      def row(id: Int, x: Double, y: Double, radius: Double): Row =
        Row(id, GeographyConstructors.geogFromWKT(s"POINT ($x $y)", srid), radius)
      session
        .createDataFrame(
          Seq(
            row(1, 179.9999, 0, 100),
            row(2, 179, 89.9999, 1),
            row(3, 179, -89.9999, 1),
            row(4, 0, 0, 2000),
            Row(5, null, 100d)).asJava,
          schema)
        .createOrReplaceTempView("geog_distance_left")
      session
        .createDataFrame(
          Seq(
            row(10, -179.9999, 0, 100),
            row(20, -179, 89.9999, 1),
            row(30, -179, -89.9999, 1),
            row(40, 0.005, 0, 2000)).asJava,
          schema)
        .createOrReplaceTempView("geog_distance_right")
      val hint = if (broadcast) "/*+ BROADCAST(l) */" else ""
      for (operator <- Seq("<", "<="); radius <- Seq("100.0", "r.radius")) {
        def query = session.sql(s"""SELECT $hint l.id, r.id
          FROM geog_distance_left l JOIN geog_distance_right r
          ON ST_Distance(l.g, r.g) $operator $radius""")
        session.conf.set("sedona.join.optimizationmode", "none")
        val expected = query.collect().map(r => (r.getInt(0), r.getInt(1))).toSet
        assert(Set((1, 10), (2, 20), (3, 30)).subsetOf(expected))
        session.conf.set("sedona.join.optimizationmode", "all")
        val actual = query
        val plan = actual.queryExecution.executedPlan
        if (broadcast && radius == "100.0") {
          assert(plan.exists {
            case join: BroadcastIndexJoinExec => join.geographyShape
            case _ => false
          })
        } else {
          assert(
            !plan.exists(_.isInstanceOf[DistanceJoinExec]),
            "Geography has no partitioned S2 join; use Spark's exact scalar fallback")
          assert(!plan.exists(_.isInstanceOf[BroadcastIndexJoinExec]))
        }
        assert(actual.collect().map(r => (r.getInt(0), r.getInt(1))).toSet == expected)
      }
    }
  }

  Seq(false, true).foreach { broadcast =>
    test(
      s"nonfinite, null, negative and row-dependent bounds preserve Spark comparison semantics with broadcast=$broadcast") {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("g", GeographyUDT()),
          StructField("radius", DoubleType)))
      val left = GeographyConstructors.geogFromWKT("POINT (1 2)", 4326)
      val right = GeographyConstructors.geogFromWKT("POINT (1.0001 2)", 4326)
      session
        .createDataFrame(Seq(Row(1, left, Double.NaN)).asJava, schema)
        .createOrReplaceTempView("geog_distance_left")
      val radii: Seq[java.lang.Double] =
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity, -1d, null, 0d, 100d)
      session
        .createDataFrame(
          radii.zipWithIndex.map { case (radius, id) =>
            Row(id + 10, right, radius)
          }.asJava,
          schema)
        .createOrReplaceTempView("geog_distance_right")
      val hint = if (broadcast) "/*+ BROADCAST(l) */" else ""
      for (operator <- Seq("<", "<=");
        radius <- Seq(
          "r.radius",
          "l.radius",
          "cast('NaN' as double)",
          "cast('Infinity' as double)",
          "cast('-Infinity' as double)",
          "cast(null as double)",
          "-1.0")) {
        def query = session.sql(s"""SELECT $hint l.id, r.id
          FROM geog_distance_left l JOIN geog_distance_right r
          ON ST_Distance(l.g, r.g) $operator $radius""")
        session.conf.set("sedona.join.optimizationmode", "none")
        val expected = query.collect().map(r => (r.getInt(0), r.getInt(1))).toSet
        if (radius.contains("NaN") || radius == "l.radius") assert(expected.nonEmpty)
        session.conf.set("sedona.join.optimizationmode", "all")
        val actual = query
        assert(!actual.queryExecution.executedPlan.exists(_.isInstanceOf[BroadcastIndexJoinExec]))
        assert(!actual.queryExecution.executedPlan.exists(_.isInstanceOf[DistanceJoinExec]))
        assert(actual.collect().map(r => (r.getInt(0), r.getInt(1))).toSet == expected)
      }
    }
  }
}
