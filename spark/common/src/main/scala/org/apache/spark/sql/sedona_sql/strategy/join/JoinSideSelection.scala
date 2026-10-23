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

import org.apache.sedona.core.enums.{JoinBuildSide, JoinSpartitionDominantSide}

/**
 * Chooses the sides of a partitioned spatial join: the dominant side, whose samples build the
 * spatial partitioning grid, and the side the per-partition spatial index is built on.
 */
object JoinSideSelection {

  /**
   * Returns the (dominant side, index build side) of a join. A side the user configured is kept
   * as is. A side the user did not configure is the larger of the two inputs, so the grid is
   * built from the larger dataset and the index holds the larger dataset; the left side is used
   * on a tie or when the sizes are unknown (a count of -1), which matches the previous default.
   *
   * @param leftCount
   *   approximate number of rows on the left side, or -1 if unknown
   * @param rightCount
   *   approximate number of rows on the right side, or -1 if unknown
   * @param configuredDominantSide
   *   the dominant side set by the user, if any
   * @param configuredBuildSide
   *   the index build side set by the user, if any
   */
  def chooseSides(
      leftCount: Long,
      rightCount: Long,
      configuredDominantSide: Option[JoinSpartitionDominantSide],
      configuredBuildSide: Option[JoinBuildSide]): (JoinSpartitionDominantSide, JoinBuildSide) = {
    val rightIsLarger = rightCount > leftCount
    val dominantSide = configuredDominantSide.getOrElse(
      if (rightIsLarger) JoinSpartitionDominantSide.RIGHT else JoinSpartitionDominantSide.LEFT)
    val buildSide = configuredBuildSide.getOrElse(
      if (rightIsLarger) JoinBuildSide.RIGHT else JoinBuildSide.LEFT)
    (dominantSide, buildSide)
  }
}
