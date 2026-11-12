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
package org.apache.sedona.stats

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.datasources.geoparquet.internal.ResolveDefaultColumns
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class NativeStatsHashSuite extends AnyFunSuite {
  private val point = "0101000000000000000000F03F0000000000000040"
  private def geometry(srid: Int): Column =
    call_function("ST_SetSRID", call_function("ST_GeomFromWKB", unhex(lit(point))), lit(srid))
  private def geography(srid: Int): Column =
    call_function("ST_SetSRID", call_function("ST_GeogFromWKB", unhex(lit(point))), lit(srid))
  private def evaluate(column: Column): String = {
    val conf = new SQLConf
    conf.setConfString("spark.sql.geospatial.enabled", "true")
    SQLConf.withExistingConf(conf) {
      val parsed =
        new SparkSqlParser().parseExpression(
          org.apache.spark.sql.NativeStatsColumnSql.sql(column))
      val analyzer = ResolveDefaultColumns.DefaultColumnAnalyzer
      val analyzed = analyzer.execute(Project(Seq(Alias(parsed, "hash")()), OneRowRelation()))
      analyzer.checkAnalysis(analyzed)
      val finished = ResolveDefaultColumns.DefaultColumnOptimizer.FinishAnalysis(analyzed)
      val expression =
        finished.asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
      expression.eval().toString
    }
  }
  private def hash(value: Column, dataType: DataType): String =
    evaluate(
      SpatialRowHash
        .hash(struct(value.as("value")), StructType(Seq(StructField("value", dataType)))))

  test("unadapted native geometry JSON remains an upstream unsupported path") {
    val error = intercept[Exception] {
      evaluate(to_json(struct(geometry(4326).as("geometry"))))
    }
    assert(error.getMessage.toLowerCase.contains("geometry"))
  }

  test("native geometry hash includes SRID and is deterministic") {
    val wgs = hash(geometry(4326), GeometryType("ANY"))
    assert(wgs == hash(geometry(4326), GeometryType("ANY")))
    assert(wgs != hash(geometry(3857), GeometryType("ANY")))
    assert(wgs != hash(lit(null).cast(GeometryType("ANY")), GeometryType("ANY")))
  }

  test("native geography hash includes its CRS and null remains null") {
    assert(
      hash(geography(4326), GeographyType("ANY")) !=
        hash(geography(4269), GeographyType("ANY")))
    val value =
      SpatialRowHash.hashableColumn(lit(null).cast(GeographyType(4326)), GeographyType(4326))
    assert(evaluate(to_json(struct(value.as("value")))) == "{}")
  }

  test("nested arrays maps and structs preserve native values and SQL null containers") {
    val schema = StructType(
      Seq(
        StructField("points", ArrayType(GeometryType("ANY"))),
        StructField("globe", MapType(StringType, GeographyType("ANY")))))
    def nested(srid: Int) = struct(
      array(geometry(srid), lit(null).cast(GeometryType("ANY"))).as("points"),
      map(lit("earth"), geography(4326)).as("globe"))
    assert(hash(nested(4326), schema) == hash(nested(4326), schema))
    assert(hash(nested(4326), schema) != hash(nested(3857), schema))
    assert(hash(lit(null).cast(schema), schema) != hash(nested(4326), schema))
    val nullMap = lit(null).cast(MapType(StringType, GeographyType("ANY")))
    val nullValue = SpatialRowHash
      .hashableColumn(nullMap, MapType(StringType, GeographyType("ANY")))
      .as("value")
    assert(evaluate(to_json(struct(nullValue))) == "{}")
  }

  test("nonspatial container hashing keeps the original JSON expression and result") {
    val dataType = StructType(Seq(StructField("values", MapType(StringType, IntegerType))))
    val value = struct(map(lit("b"), lit(2), lit("a"), lit(1)).as("values"))
    assert(SpatialRowHash.hashableColumn(value, dataType) eq value)
    assert(evaluate(SpatialRowHash.hash(value, dataType)) == evaluate(sha2(to_json(value), 256)))
  }
}
