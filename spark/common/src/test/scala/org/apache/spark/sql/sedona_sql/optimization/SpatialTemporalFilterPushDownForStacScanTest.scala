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
package org.apache.spark.sql.sedona_sql.optimization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GreaterThanOrEqual, LessThanOrEqual, Literal}
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.getFilterTemporal
import org.apache.spark.sql.types.TimestampType
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDateTime, ZoneOffset}

class SpatialTemporalFilterPushDownForStacScanTest extends AnyFunSuite {

  private lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("SpatialTemporalFilterPushDownForStacScanTest")
      .getOrCreate()

  private lazy val pushDown = new SpatialTemporalFilterPushDownForStacScan(spark)

  /** Spark stores TimestampType internally as microseconds since the Unix epoch. */
  private def toMicros(isoLocal: String): Long = {
    val instant = LocalDateTime.parse(isoLocal).toInstant(ZoneOffset.UTC)
    instant.getEpochSecond * 1000000L + instant.getNano / 1000L
  }

  private val datetime = AttributeReference("datetime", TimestampType)()

  // The push-down must not truncate the microsecond bound to milliseconds (it once divided by
  // 1000, pushing 23:59:59.999999Z as 23:59:59.999Z), and the serialized inclusive upper bound is
  // widened to the last nanosecond of its microsecond so an item Spark would truncate into range
  // (e.g. 23:59:59.999999500Z) is not dropped by the remote catalog (issue #3110).
  test("push-down widens an inclusive upper bound to nanosecond precision") {
    val predicate =
      LessThanOrEqual(datetime, Literal(toMicros("2020-05-31T23:59:59.999999"), TimestampType))
    val filters = pushDown.translateToTemporalFilters(Seq(predicate))
    assert(filters.size == 1)
    assert(getFilterTemporal(filters.head) == "datetime=../2020-05-31T23:59:59.999999999Z")
  }

  test("push-down keeps a lower bound exact") {
    val predicate =
      GreaterThanOrEqual(datetime, Literal(toMicros("2020-05-01T00:00:00.000001"), TimestampType))
    val filters = pushDown.translateToTemporalFilters(Seq(predicate))
    assert(filters.size == 1)
    assert(getFilterTemporal(filters.head) == "datetime=2020-05-01T00:00:00.000001000Z/..")
  }

  test("push-down combines an exact lower bound with a widened upper bound") {
    val lower =
      GreaterThanOrEqual(datetime, Literal(toMicros("2020-05-01T00:00:00.000000"), TimestampType))
    val upper =
      LessThanOrEqual(datetime, Literal(toMicros("2020-05-31T23:59:59.999999"), TimestampType))
    val filters = pushDown.translateToTemporalFilters(Seq(lower, upper))
    val combined = filters.reduce(TemporalFilter.AndFilter)
    assert(
      getFilterTemporal(combined) ==
        "datetime=2020-05-01T00:00:00.000000000Z/2020-05-31T23:59:59.999999999Z")
  }

  test("push-down upper bound covers 7-to-9 digit sub-microsecond timestamps") {
    // STAC permits up to nine fractional digits. Spark truncates any such item to microseconds,
    // so every item in the bound's final microsecond survives the residual `<= .999999` filter and
    // must therefore also fall within the pushed remote bound. Assert that legal 7-, 8- and 9-digit
    // timestamps in that microsecond are not after the widened remote upper bound.
    val predicate =
      LessThanOrEqual(datetime, Literal(toMicros("2020-05-31T23:59:59.999999"), TimestampType))
    val url = getFilterTemporal(pushDown.translateToTemporalFilters(Seq(predicate)).head)
    assert(url == "datetime=../2020-05-31T23:59:59.999999999Z")

    val remoteEnd = LocalDateTime.parse(url.stripPrefix("datetime=../").stripSuffix("Z"))
    Seq(
      "2020-05-31T23:59:59.9999995", // 7 digits
      "2020-05-31T23:59:59.99999999", // 8 digits
      "2020-05-31T23:59:59.999999999" // 9 digits
    ).foreach { ts =>
      assert(
        !LocalDateTime.parse(ts).isAfter(remoteEnd),
        s"$ts must fall within the pushed remote bound $remoteEnd")
    }
  }
}
