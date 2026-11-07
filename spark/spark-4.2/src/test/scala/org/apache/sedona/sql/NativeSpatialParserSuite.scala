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

import org.apache.sedona.sql.parser.SedonaSqlParser
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types.{GeographyType, GeometryType}
import org.scalatest.FunSuite

class NativeSpatialParserSuite extends FunSuite {
  private val parser = new SedonaSqlParser(new SparkSqlParser)

  test("Sedona parser preserves native spatial types with explicit SRIDs") {
    val plan =
      parser.parsePlan("SELECT CAST(NULL AS GEOMETRY(3857)), CAST(NULL AS GEOGRAPHY(4326))")
    val types = plan.expressions.flatMap(_.collect { case cast: Cast => cast.dataType })
    assert(types.exists(_.isInstanceOf[GeometryType]))
    assert(types.exists(_.isInstanceOf[GeographyType]))
  }

  test("Sedona fallback does not reintroduce the legacy bare GEOMETRY type") {
    intercept[ParseException] {
      parser.parsePlan("SELECT CAST(NULL AS GEOMETRY)")
    }
  }
}
