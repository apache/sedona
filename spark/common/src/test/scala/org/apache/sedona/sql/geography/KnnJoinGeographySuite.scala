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
package org.apache.sedona.sql.geography

import org.apache.sedona.sql.TestBaseScala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, expr, lit}
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.ST_KNN
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastGeographyKNNJoinExec, LeftSide, RightSide}

import java.util.Random

/**
 * ST_KNN(geography, geography, k) joins. Neighbours are ranked by the same distance that
 * ST_Distance(geography, geography) returns, so most tests compare the join against a brute-force
 * cross join ranked by ST_Distance.
 */
class KnnJoinGeographySuite extends TestBaseScala {

  private def queriesDf(rows: Seq[(Int, String)]): DataFrame = {
    import sparkSession.implicits._
    rows.toDF("qid", "wkt").selectExpr("qid", "ST_GeogFromWKT(wkt, 4326) AS qgeog")
  }

  private def objectsDf(rows: Seq[(Int, String)]): DataFrame = {
    import sparkSession.implicits._
    rows.toDF("oid", "wkt").selectExpr("oid", "ST_GeogFromWKT(wkt, 4326) AS ogeog")
  }

  private def knnExecs(df: DataFrame): Seq[BroadcastGeographyKNNJoinExec] =
    df.queryExecution.sparkPlan.collect { case p: BroadcastGeographyKNNJoinExec => p }

  private def pairs(df: DataFrame): Seq[(Int, Int)] =
    df.selectExpr("qid", "oid").collect().map(r => (r.getInt(0), r.getInt(1))).sorted.toSeq

  /** The sorted distances from each query to the neighbours the join returned for it. */
  private def joinedDistances(joined: DataFrame): Map[Int, Seq[Double]] =
    joined
      .selectExpr("qid", "ST_Distance(qgeog, ogeog) AS d")
      .collect()
      .groupBy(_.getInt(0))
      .map { case (qid, rows) => qid -> rows.map(_.getDouble(1)).sorted.toSeq }

  /** The k smallest ST_Distance values per query, computed by a brute-force cross join. */
  private def bruteForceDistances(
      queries: DataFrame,
      objects: DataFrame,
      k: Int): Map[Int, Seq[Double]] =
    queries
      .crossJoin(objects)
      .selectExpr("qid", "ST_Distance(qgeog, ogeog) AS d")
      .collect()
      .groupBy(_.getInt(0))
      .map { case (qid, rows) => qid -> rows.map(_.getDouble(1)).sorted.take(k).toSeq }

  // Points spread over the whole globe, with extra points close to the antimeridian and the
  // poles, where a planar longitude / latitude search goes wrong.
  private def randomPoints(count: Int, seed: Long): Seq[(Int, String)] = {
    val random = new Random(seed)
    (0 until count).map { i =>
      val (lon, lat) = i % 5 match {
        case 0 =>
          (
            if (random.nextBoolean()) 179.0 + random.nextDouble()
            else -180.0 + random.nextDouble(),
            random.nextDouble() * 120 - 60)
        case 1 => (random.nextDouble() * 360 - 180, 88.0 + random.nextDouble() * 2)
        case 2 => (random.nextDouble() * 360 - 180, -90.0 + random.nextDouble() * 2)
        case _ => (random.nextDouble() * 360 - 180, random.nextDouble() * 170 - 85)
      }
      (i, s"POINT($lon $lat)")
    }
  }

  // Small boxes spread over the globe, some of them crossing the antimeridian.
  private def randomPolygons(count: Int, seed: Long): Seq[(Int, String)] = {
    val random = new Random(seed)
    (0 until count).map { i =>
      val y = random.nextDouble() * 150 - 75
      val h = 0.2 + random.nextDouble() * 2
      val w = 0.2 + random.nextDouble() * 2
      val wkt = if (i % 6 == 0) {
        val west = 180.0 - w / 2
        val east = -180.0 + w / 2
        s"POLYGON(($west $y, $east $y, $east ${y + h}, $west ${y + h}, $west $y))"
      } else {
        val x = random.nextDouble() * 350 - 177
        s"POLYGON(($x $y, ${x + w} $y, ${x + w} ${y + h}, $x ${y + h}, $x $y))"
      }
      (i, wkt)
    }
  }

  private val knn3 = expr("ST_KNN(qgeog, ogeog, 3)")

  describe("ST_KNN(geography, geography, k) broadcast join planning") {

    it("plans a geography KNN join that broadcasts the object side") {
      val queries = queriesDf(randomPoints(10, 1))
      val objects = objectsDf(randomPoints(20, 2))
      val joined = queries.join(broadcast(objects), knn3)
      val execs = knnExecs(joined)
      assert(execs.size === 1, joined.queryExecution.sparkPlan.toString)
      assert(execs.head.broadcastSide === RightSide)
      assert(execs.head.querySide === LeftSide)
    }

    it("plans a geography KNN join that broadcasts the query side") {
      val queries = queriesDf(randomPoints(10, 1))
      val objects = objectsDf(randomPoints(20, 2))
      val joined = broadcast(queries).join(objects, knn3)
      val execs = knnExecs(joined)
      assert(execs.size === 1, joined.queryExecution.sparkPlan.toString)
      assert(execs.head.broadcastSide === LeftSide)
      assert(execs.head.querySide === LeftSide)
    }

    it("auto-broadcasts when sedona.join.autoBroadcastJoinThreshold permits") {
      withConf(Map("sedona.join.autoBroadcastJoinThreshold" -> "10485760")) {
        val queries = queriesDf(randomPoints(10, 1))
        val objects = objectsDf(randomPoints(20, 2))
        val joined = queries.join(objects, knn3)
        assert(knnExecs(joined).size === 1, joined.queryExecution.sparkPlan.toString)
        assert(joined.count() === 30)
      }
    }

    it("runs the example from the documentation") {
      sparkSession
        .sql("""SELECT id, name, ST_GeogFromWKT(wkt, 4326) AS geog FROM VALUES
                |(1, 'Suva', 'POINT (178.44 -18.14)'),
                |(2, 'Longyearbyen', 'POINT (15.63 78.22)')
                |AS t(id, name, wkt)""".stripMargin)
        .createOrReplaceTempView("knn_doc_stations")
      sparkSession
        .sql("""SELECT id, name, ST_GeogFromWKT(wkt, 4326) AS geog FROM VALUES
                |(1, 'Apia', 'POINT (-171.76 -13.83)'),
                |(2, 'Noumea', 'POINT (166.46 -22.27)'),
                |(3, 'Alert', 'POINT (-62.35 82.50)'),
                |(4, 'Tromso', 'POINT (18.96 69.65)')
                |AS t(id, name, wkt)""".stripMargin)
        .createOrReplaceTempView("knn_doc_airports")
      val joined = sparkSession.sql("""
          |SELECT /*+ BROADCAST(a) */
          |    s.name AS station,
          |    a.name AS airport,
          |    ROUND(ST_Distance(s.geog, a.geog) / 1000) AS km
          |FROM knn_doc_stations AS s
          |JOIN knn_doc_airports AS a
          |ON ST_KNN(s.geog, a.geog, 1)
          |ORDER BY station""".stripMargin)
      assert(knnExecs(joined).size === 1, joined.queryExecution.sparkPlan.toString)
      val rows = joined.collect().map(r => (r.getString(0), r.getString(1), r.getDouble(2))).toSeq
      assert(rows === Seq(("Longyearbyen", "Tromso", 958.0), ("Suva", "Apia", 1152.0)))
    }

    it("fails at planning time when neither side is broadcast") {
      val queries = queriesDf(randomPoints(10, 1))
      val objects = objectsDf(randomPoints(20, 2))
      val exception = intercept[UnsupportedOperationException] {
        queries.join(objects, knn3).queryExecution.sparkPlan
      }
      assert(exception.getMessage.contains("ST_KNN on geography requires a broadcast join"))
    }

    it("fails at planning time for a join type other than inner") {
      val queries = queriesDf(randomPoints(10, 1))
      val objects = objectsDf(randomPoints(20, 2))
      val exception = intercept[UnsupportedOperationException] {
        queries.join(broadcast(objects), knn3, "left_outer").queryExecution.sparkPlan
      }
      assert(exception.getMessage.contains("ST_KNN on geography supports inner joins only"))
    }

    it("rejects ST_KNN(geog, geog, k, useSpheroid) at analysis time") {
      // Spherical ranking is the definition for geography, so there is no 4-argument overload.
      val queries = queriesDf(randomPoints(10, 1))
      val objects = objectsDf(randomPoints(20, 2))
      val message = analysisFailure {
        queries.join(broadcast(objects), expr("ST_KNN(qgeog, ogeog, 3, true)"))
      }
      assert(message.contains("st_knn"), message)
      assert(
        message.contains("datatype_mismatch") || message.contains("data type mismatch"),
        message)
    }

    it("rejects mixed geometry and geography arguments at analysis time") {
      val queries = queriesDf(randomPoints(10, 1))
      val objects = objectsDf(randomPoints(20, 2)).selectExpr("oid", "ST_Point(1, 2) AS ogeom")
      val message = analysisFailure {
        queries.join(broadcast(objects), expr("ST_KNN(qgeog, ogeom, 3)"))
      }
      assert(message.contains("st_knn"), message)
      assert(message.contains("types of arguments does not match"), message)
    }
  }

  private def analysisFailure(body: => DataFrame): String = {
    val ex = intercept[Throwable] {
      body.queryExecution.sparkPlan
    }
    Iterator
      .iterate[Throwable](ex)(t => if (t == null) null else t.getCause)
      .takeWhile(_ != null)
      .map(_.getMessage)
      .mkString(" | ")
      .toLowerCase(java.util.Locale.ROOT)
  }

  // (name, how to build the join given queries, objects and the join condition)
  private val broadcastSides
      : Seq[(String, (DataFrame, DataFrame, org.apache.spark.sql.Column) => DataFrame)] = Seq(
    (
      "object side broadcast",
      (
          queries,
          objects,
          condition) => queries.repartition(4).join(broadcast(objects), condition)),
    (
      "query side broadcast",
      (
          queries,
          objects,
          condition) => broadcast(queries).join(objects.repartition(4), condition)),
    (
      "object side broadcast, objects first",
      (
          queries,
          objects,
          condition) => broadcast(objects).join(queries.repartition(4), condition)),
    (
      "query side broadcast, objects first",
      (
          queries,
          objects,
          condition) => objects.repartition(4).join(broadcast(queries), condition)))

  for ((side, join) <- broadcastSides) {
    describe(s"ST_KNN(geography, geography, k) join results: $side") {

      it("ranks point neighbours by ST_Distance") {
        val queries = queriesDf(randomPoints(60, 11))
        val objects = objectsDf(randomPoints(400, 12))
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 5)"))
        assert(knnExecs(joined).size === 1, joined.queryExecution.sparkPlan.toString)
        val actual = joinedDistances(joined)
        assert(actual.size === 60)
        assert(actual === bruteForceDistances(queries, objects, 5))
      }

      it("ranks polygon neighbours by ST_Distance") {
        val queries = queriesDf(randomPoints(60, 21))
        val objects = objectsDf(randomPolygons(300, 22))
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 5)"))
        val actual = joinedDistances(joined)
        assert(actual.size === 60)
        assert(actual === bruteForceDistances(queries, objects, 5))
      }

      it("ranks neighbours of polygon queries by ST_Distance") {
        val queries = queriesDf(randomPolygons(40, 31))
        val objects = objectsDf(randomPolygons(200, 32) ++ randomPoints(200, 33).map {
          case (id, wkt) => (id + 1000, wkt)
        })
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 4)"))
        val actual = joinedDistances(joined)
        assert(actual.size === 40)
        assert(actual === bruteForceDistances(queries, objects, 4))
      }

      it("uses the minimum distance to a polygon, not the distance to its centroid") {
        val queries = queriesDf(Seq((1, "POINT(0 0)")))
        val objects = objectsDf(
          Seq(
            // Nearest edge is 0.1 degrees away; the centroid is 5 degrees away.
            (10, "POLYGON((0.1 -5, 10 -5, 10 5, 0.1 5, 0.1 -5))"),
            (11, "POINT(1 0)")))
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 1)"))
        assert(pairs(joined) === Seq((1, 10)))
      }

      it("finds the nearest neighbour across the antimeridian") {
        val queries = queriesDf(Seq((1, "POINT(179.9 0)"), (2, "POINT(-179.9 10)")))
        val objects = objectsDf(
          Seq(
            (10, "POINT(-179.9 0)"), // 0.2 degrees from query 1, across the antimeridian
            (11, "POINT(175 0)"), // 4.9 degrees from query 1
            (12, "POINT(179.9 10)"), // 0.2 degrees from query 2, across the antimeridian
            (13, "POINT(-175 10)")
          )
        ) // 4.9 degrees from query 2
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 1)"))
        assert(pairs(joined) === Seq((1, 10), (2, 12)))
      }

      it("finds the nearest neighbour across a pole") {
        val queries = queriesDf(Seq((1, "POINT(0 89.9)"), (2, "POINT(30 -89.9)")))
        val objects = objectsDf(
          Seq(
            (10, "POINT(180 89.9)"), // 0.2 degrees from query 1, over the north pole
            (11, "POINT(0 88)"), // 1.9 degrees from query 1
            (12, "POINT(-150 -89.9)"), // 0.2 degrees from query 2, over the south pole
            (13, "POINT(30 -88)")
          )
        ) // 1.9 degrees from query 2
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 1)"))
        assert(pairs(joined) === Seq((1, 10), (2, 12)))
      }

      it("ranks by distance on the sphere where planar degrees disagree") {
        // At 60 degrees north a degree of longitude is half as long as a degree of latitude:
        // object 10 is about 111 km away, object 11 about 167 km.
        val queries = queriesDf(Seq((1, "POINT(0 60)")))
        val objects = objectsDf(Seq((10, "POINT(2 60)"), (11, "POINT(0 61.5)")))
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 1)"))
        assert(pairs(joined) === Seq((1, 10)))
      }

      it("returns every object when k exceeds the number of objects") {
        val queries = queriesDf(Seq((1, "POINT(0 0)"), (2, "POINT(50 50)")))
        val objects = objectsDf(Seq((10, "POINT(1 1)"), (11, "POINT(2 2)"), (12, "POINT(3 3)")))
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 10)"))
        assert(pairs(joined) === Seq((1, 10), (1, 11), (1, 12), (2, 10), (2, 11), (2, 12)))
      }

      it("skips NULL and empty geographies on both sides") {
        val queries =
          queriesDf(Seq((1, "POINT(0 0)"), (2, null), (3, "POINT EMPTY"), (4, "POLYGON EMPTY")))
        val objects = objectsDf(
          Seq(
            (10, "POINT(1 1)"),
            (11, null),
            (12, "POINT EMPTY"),
            (13, "POLYGON EMPTY"),
            (14, "POINT(5 5)")))
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 4)"))
        // NULL and empty queries get no neighbours; NULL and empty objects are never neighbours.
        assert(pairs(joined) === Seq((1, 10), (1, 14)))
      }

      it("returns exactly k neighbours when tie breakers are excluded") {
        withConf(Map("spark.sedona.join.knn.includeTieBreakers" -> "false")) {
          val queries = queriesDf(Seq((1, "POINT(0 0)")))
          val objects = objectsDf(
            Seq((10, "POINT(1 0)"), (11, "POINT(1 0)"), (12, "POINT(1 0)"), (13, "POINT(3 0)")))
          val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 2)"))
          val result = pairs(joined)
          assert(result.size === 2)
          assert(result.forall { case (_, oid) => oid <= 12 })
        }
      }

      it("returns all neighbours tied at the k-th distance when tie breakers are included") {
        withConf(Map("spark.sedona.join.knn.includeTieBreakers" -> "true")) {
          val queries = queriesDf(Seq((1, "POINT(0 0)")))
          val objects = objectsDf(
            Seq(
              (10, "POINT(1 0)"),
              (11, "POINT(1 0)"),
              (12, "POINT(1 0)"),
              (13, "POINT(3 0)"),
              (14, "POINT(0.5 0)")))
          val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 2)"))
          assert(pairs(joined) === Seq((1, 10), (1, 11), (1, 12), (1, 14)))
        }
      }

      it("applies residual conditions to the k nearest neighbours") {
        import sparkSession.implicits._
        val queries = Seq((1, "POINT(0 0)", 5), (2, "POINT(10 10)", 50))
          .toDF("qid", "wkt", "qscore")
          .selectExpr("qid", "ST_GeogFromWKT(wkt, 4326) AS qgeog", "qscore")
        val objects = Seq((10, "POINT(0.1 0.1)", 10), (11, "POINT(10.1 10.1)", 10))
          .toDF("oid", "wkt", "oscore")
          .selectExpr("oid", "ST_GeogFromWKT(wkt, 4326) AS ogeog", "oscore")
        // The residual filters the neighbours that were found; it does not make the join look
        // further for a neighbour that passes it.
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 1) AND qscore > oscore"))
        assert(knnExecs(joined).size === 1, joined.queryExecution.sparkPlan.toString)
        assert(pairs(joined) === Seq((2, 11)))
      }

      it("supports the DataFrame API") {
        val queries = queriesDf(Seq((1, "POINT(0 0)")))
        val objects = objectsDf(Seq((10, "POINT(1 1)"), (11, "POINT(2 2)"), (12, "POINT(3 3)")))
        val joined = join(queries, objects, ST_KNN(col("qgeog"), col("ogeog"), lit(2)))
        assert(knnExecs(joined).size === 1, joined.queryExecution.sparkPlan.toString)
        assert(pairs(joined) === Seq((1, 10), (1, 11)))
      }

      it("returns no rows when the object side is empty") {
        val queries = queriesDf(Seq((1, "POINT(0 0)")))
        val objects = objectsDf(Seq((10, "POINT(1 1)"))).where("oid < 0")
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 2)"))
        assert(joined.count() === 0)
      }

      it("returns no rows when the query side is empty") {
        val queries = queriesDf(Seq((1, "POINT(0 0)"))).where("qid < 0")
        val objects = objectsDf(Seq((10, "POINT(1 1)")))
        val joined = join(queries, objects, expr("ST_KNN(qgeog, ogeog, 2)"))
        assert(joined.count() === 0)
      }
    }
  }
}
