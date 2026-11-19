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

import org.apache.sedona.core.enums.{JoinBuildSide, JoinSpartitionDominantSide}
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.sedona_sql.strategy.join.JoinSideSelection
import org.scalatest.funsuite.AnyFunSuite

class JoinSideSelectionSuite extends AnyFunSuite {

  private val Left = (JoinSpartitionDominantSide.LEFT, JoinBuildSide.LEFT)
  private val Right = (JoinSpartitionDominantSide.RIGHT, JoinBuildSide.RIGHT)

  test("uses the larger side for both sides when neither is configured") {
    assert(JoinSideSelection.chooseSides(10, 1000, None, None) == Right)
    assert(JoinSideSelection.chooseSides(1000, 10, None, None) == Left)
  }

  test("uses the left side on a tie or when the sizes are unknown") {
    assert(JoinSideSelection.chooseSides(500, 500, None, None) == Left)
    assert(JoinSideSelection.chooseSides(-1, -1, None, None) == Left)
  }

  test("keeps a configured dominant side and chooses only the build side") {
    assert(
      JoinSideSelection.chooseSides(10, 1000, Some(JoinSpartitionDominantSide.LEFT), None) ==
        (JoinSpartitionDominantSide.LEFT, JoinBuildSide.RIGHT))
  }

  test("keeps a configured build side and chooses only the dominant side") {
    assert(
      JoinSideSelection.chooseSides(10, 1000, None, Some(JoinBuildSide.LEFT)) ==
        (JoinSpartitionDominantSide.RIGHT, JoinBuildSide.LEFT))
  }

  test("keeps both configured sides whatever the sizes") {
    assert(
      JoinSideSelection.chooseSides(
        10,
        1000,
        Some(JoinSpartitionDominantSide.LEFT),
        Some(JoinBuildSide.LEFT)) == Left)
  }

  test("SedonaConf reports which join sides the user configured") {
    val unset = new SedonaConf(new SparkConf(false))
    assert(!unset.isJoinSpartitionDominantSideConfigured)
    assert(!unset.isJoinBuildSideConfigured)

    val sedonaKeys = new SedonaConf(
      new SparkConf(false)
        .set("sedona.join.spatitionside", "right")
        .set("sedona.join.indexbuildside", "right"))
    assert(sedonaKeys.isJoinSpartitionDominantSideConfigured)
    assert(sedonaKeys.isJoinBuildSideConfigured)
    assert(sedonaKeys.getJoinSpartitionDominantSide == JoinSpartitionDominantSide.RIGHT)
    assert(sedonaKeys.getJoinBuildSide == JoinBuildSide.RIGHT)

    val sparkSedonaKey =
      new SedonaConf(new SparkConf(false).set("spark.sedona.join.indexbuildside", "left"))
    assert(!sparkSedonaKey.isJoinSpartitionDominantSideConfigured)
    assert(sparkSedonaKey.isJoinBuildSideConfigured)
  }
}
