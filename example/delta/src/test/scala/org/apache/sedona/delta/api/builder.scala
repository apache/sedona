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

package org.apache.sedona.delta.api

import org.apache.sedona.delta.Base
import org.apache.spark.sql.DataFrame
import org.locationtech.jts.geom.Geometry


trait MeasureTestDataFixture {
  self: Base =>
  import spark.implicits._
  protected val measurementDataColumns = Seq("index", "geom", "measure", "city")

  val Cracow: MeasureTestData = MeasureTestData(
    index = 1, geom=wktReader.read("POINT(19.936160 50.061486)"), city="Cracow", measure = 0.0
  )

  val Amsterdam: MeasureTestData = MeasureTestData(
    index = 1, geom=wktReader.read("POINT(4.899059 52.363573)"), city="Amsterdam", measure = 0.0
  )

  val LosAngeles: MeasureTestData = MeasureTestData(
    index = 1, geom=wktReader.read("POINT(-118.240286 34.004080)"), city="Los Angeles", measure = 0.0
  )
  val Washington: MeasureTestData = MeasureTestData(
    index = 1, geom=wktReader.read("POINT(-77.054092 38.934715)"), city="Washington", measure = 0.0
  )

  val Barcelona: MeasureTestData = MeasureTestData(
    index = 1, geom=wktReader.read("POINT(2.152872 41.377910)"), city="Barcelona", measure = 0.0
  )

  val Warsaw: MeasureTestData = MeasureTestData(
    index = 1, geom=wktReader.read("POINT(21.037308 52.226236)"), city="Warsaw", measure = 0.0
  )

  val Berlin: MeasureTestData = MeasureTestData(
    index = 1, geom=wktReader.read("POINT(13.419083 52.504049)"), city="Berlin", measure = 0.0
  )

  val NewYork: MeasureTestData = MeasureTestData(
    index = 1, geom=wktReader.read("POINT(-74.047214 40.742164)"), city="New York", measure = 0.0
  )

  val polandExtent: String = """
      | POLYGON((
      |   14.050961 49.847483,
      |   14.345359 54.647904,
      |   23.485700 54.882453,
      |   24.761422 49.274621,
      |   14.050961 49.847483
      |))
      """.stripMargin.replace("  ", "").replace("\n", "")

  val europeExtent: String =
    s"""
      | POLYGON((
      | -21.289062499999996 73.22863503912812,
      | 43.046875 73.22863503912812,
      | 43.046875 38.82781590516771,
      | -21.289062499999996 38.82781590516771,
      | -21.289062499999996 73.22863503912812
      | ))
      |""".stripMargin.replace("  ", "").replace("\n", "")

  def verifyPlan(predicate: String): Boolean = {
    val numberOfStages = spark.sharedState.statusStore.executionsCount().asInstanceOf[Int]
    val executionPlans = (0 until numberOfStages).map(
      stgNr => spark.sharedState.statusStore.execution(stgNr)
    ).flatMap(plan => plan.map(s => s.physicalPlanDescription))

    executionPlans.exists(element => element.contains(predicate))
  }

  def produceDfFromRecords(records: MeasureTestData*): DataFrame = records.map(_.toRecord).toDF(
    "index", "geom", "measure", "city"
  )

  def compareDataFrames(left: DataFrame, right: DataFrame): Boolean = {
    val countEqual = left.count == right.count

    val elementsTheSame = left.alias("result").join(
      right.alias("expected"), $"result.index" === $"expected.index"
    ).withColumn(
      "eq",
      $"expected.measure" === $"result.measure"
        && $"expected.city" === $"result.city"
        && $"expected.geom" === $"result.geom"
    ).filter("eq").count == right.count

    countEqual && elementsTheSame
  }

}

case class MeasureTestData(index: Long, city: String, measure: Double, geom: Geometry){
  def withMeasure(measure: Double): MeasureTestData = copy(measure=measure)
  def withIndex(index: Long): MeasureTestData = copy(index=index)
  def toRecord: (Long, Geometry, Double, String) = (index, geom, measure, city)

}