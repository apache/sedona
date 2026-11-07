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

import org.apache.spark.sql.Row
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.strategy.join.{BroadcastObjectSideKNNJoinExec, BroadcastQuerySideKNNJoinExec, KNNJoinExec}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

/** KNN operands bind to attributes, including expressions absent from the child plan text. */
class KnnShapeBindingTest extends TestBaseScala {
  private val expected = (0 until 4).map(i => (i, i + 10)).toSet
  private val settings = Map(
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "sedona.join.autoBroadcastJoinThreshold" -> "-1",
    "sedona.join.numpartition" -> "2")
  private var previousSettings: Map[String, Option[String]] = Map.empty

  override def beforeAll(): Unit = {
    super.beforeAll()
    previousSettings = settings.keys.map(key => key -> sparkSession.conf.getOption(key)).toMap
    settings.foreach { case (key, value) => sparkSession.conf.set(key, value) }
    sparkSession
      .sql(
        "SELECT cast(id AS INT) AS id, ST_Point(cast(id * 20 + 1 AS DOUBLE), 2D) AS g FROM range(4)")
      .createOrReplaceTempView("knn_binding_q")
    sparkSession
      .sql(
        "SELECT cast(id + 10 AS INT) AS id, ST_Point(cast(id * 20 + 1 AS DOUBLE), 2D) AS g FROM range(4)")
      .createOrReplaceTempView("knn_binding_o")
    val factory = new GeometryFactory()
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("g", GeometryUDT())))
    for ((view, offset) <- Seq("knn_binding_legacy_q" -> 0, "knn_binding_legacy_o" -> 10)) {
      val rows =
        (0 until 4).map(i => Row(i + offset, factory.createPoint(new Coordinate(i * 20 + 1, 2))))
      sparkSession.createDataFrame(sc.parallelize(rows), schema).createOrReplaceTempView(view)
    }
  }

  override def afterAll(): Unit = {
    try {
      Seq("knn_binding_q", "knn_binding_o", "knn_binding_legacy_q", "knn_binding_legacy_o")
        .foreach(sparkSession.catalog.dropTempView)
      previousSettings.foreach {
        case (key, Some(value)) => sparkSession.conf.set(key, value)
        case (key, None) => sparkSession.conf.unset(key)
      }
    } finally super.afterAll()
  }

  private def check(
      reverse: Boolean,
      computed: Boolean,
      hint: String,
      explicitUdt: Boolean): Unit = {
    val prefix = if (explicitUdt) "knn_binding_legacy" else "knn_binding"
    def shape(side: String): String = if (computed) s"ST_Buffer($side.g, 0.25D)" else s"$side.g"
    val (query, objects) = if (reverse) (shape("o"), shape("q")) else (shape("q"), shape("o"))
    val broadcast = if (hint.isEmpty) "" else s"/*+ BROADCAST($hint) */"
    val result = sparkSession.sql(
      s"SELECT $broadcast q.id, o.id FROM ${prefix}_q q JOIN ${prefix}_o o ON ST_KNN($query, $objects, 1, false)")
    val operators = result.queryExecution.executedPlan.collect {
      case _: KNNJoinExec => "partitioned"
      case _: BroadcastQuerySideKNNJoinExec => "broadcast-query"
      case _: BroadcastObjectSideKNNJoinExec => "broadcast-object"
    }
    val expectedOperator =
      if (hint.isEmpty) "partitioned"
      else if ((hint == "q") != reverse) "broadcast-query"
      else "broadcast-object"
    assert(operators == Seq(expectedOperator))
    val rows = result.collect()
    assert(rows.length == expected.size)
    assert(rows.map(row => (row.getInt(0), row.getInt(1))).toSet == expected)
  }

  for (reverse <- Seq(false, true); computed <- Seq(false, true); hint <- Seq("", "q", "o")) {
    it(
      s"binds default KNN operands by identity: reverse=$reverse computed=$computed hint=$hint") {
      check(reverse, computed, hint, explicitUdt = false)
    }
  }

  for (reverse <- Seq(false, true)) {
    it(s"retains explicitly requested GeometryUDT operands: reverse=$reverse") {
      check(reverse, computed = true, hint = "q", explicitUdt = true)
    }
  }

  for (constantFirst <- Seq(false, true)) {
    it(s"keeps reference-free KNN operands unsupported: constantFirst=$constantFirst") {
      val condition =
        if (constantFirst) "ST_KNN(ST_Point(1D, 2D), o.g, 1, false)"
        else "ST_KNN(q.g, ST_Point(1D, 2D), 1, false)"
      val error = intercept[UnsupportedOperationException] {
        sparkSession
          .sql(s"SELECT q.id, o.id FROM knn_binding_q q JOIN knn_binding_o o ON $condition")
          .queryExecution
          .executedPlan
      }
      assert(error.getMessage.contains("ST_KNN filter is not yet supported"))
    }
  }
}
