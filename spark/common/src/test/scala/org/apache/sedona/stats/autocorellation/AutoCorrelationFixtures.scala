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
package org.apache.sedona.stats.autocorellation

import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.sql.TestBaseScala

import java.nio.file.Files

trait AutoCorrelationFixtures {
  this: TestBaseScala =>
  SedonaContext.create(sparkSession)

  val positiveAutoCorrelation = List(
    (1, 1.0, 1.0, 8.5),
    (2, 1.5, 1.2, 8.2),
    (3, 1.3, 1.8, 8.7),
    (4, 1.7, 1.6, 7.9),
    (5, 4.0, 1.5, 6.2),
    (6, 4.2, 1.7, 6.5),
    (7, 4.5, 1.3, 5.9),
    (8, 4.7, 1.8, 6.0),
    (9, 1.8, 4.3, 3.1),
    (10, 1.5, 4.5, 3.4),
    (11, 1.2, 4.7, 3.0),
    (12, 1.6, 4.2, 3.3),
    (13, 1.9, 4.8, 2.8),
    (14, 4.3, 4.2, 1.2),
    (15, 4.5, 4.5, 1.5),
    (16, 4.7, 4.8, 1.0),
    (17, 4.1, 4.6, 1.3),
    (18, 4.8, 4.3, 1.1),
    (19, 4.2, 4.9, 1.4),
    (20, 4.6, 4.1, 1.6))

  val positiveCorrelationFrame = sparkSession
    .createDataFrame(positiveAutoCorrelation)
    .selectExpr("_1 as id", "_2 AS x", "_3 AS y", "_4 AS value")
    .selectExpr("id", "ST_MakePoint(x, y) AS geometry", "value")

  val negativeCorrelationPoints = List(
    (1, 1.0, 1.0, 8.5),
    (2, 2.0, 1.0, 2.1),
    (3, 3.0, 1.0, 8.2),
    (4, 4.0, 1.0, 2.3),
    (5, 5.0, 1.0, 8.7),
    (6, 1.0, 2.0, 2.5),
    (7, 2.0, 2.0, 8.1),
    (8, 3.0, 2.0, 2.7),
    (9, 4.0, 2.0, 8.3),
    (10, 5.0, 2.0, 2.0),
    (11, 1.0, 3.0, 8.6),
    (12, 2.0, 3.0, 2.2),
    (13, 3.0, 3.0, 8.4),
    (14, 4.0, 3.0, 2.4),
    (15, 5.0, 3.0, 8.0),
    (16, 1.0, 4.0, 2.6),
    (17, 2.0, 4.0, 8.8),
    (18, 3.0, 4.0, 2.8),
    (19, 4.0, 4.0, 8.9),
    (20, 5.0, 4.0, 2.9))

  val zeroCorrelationPoints = List(
    (1, 3.75, 7.89, 2.58),
    (2, 9.31, 4.25, 7.43),
    (3, 5.12, 0.48, 5.96),
    (4, 6.25, 1.74, 3.12),
    (5, 1.47, 6.33, 8.26),
    (6, 8.18, 9.57, 1.97),
    (7, 2.64, 3.05, -6.42),
    (8, 4.33, 5.88, 4.74),
    (9, 7.91, 2.41, -10.13),
    (10, 0.82, 8.76, 3.89),
    (11, 9.70, 1.19, 100.0),
    (12, 2.18, 7.54, 7.35),
    (13, 5.47, 3.94, 2.16),
    (14, 8.59, 6.78, -12.63),
    (15, 3.07, 2.88, 4.27),
    (16, 6.71, 9.12, 6.84),
    (17, 1.34, 4.51, -25.0),
    (18, 7.26, 5.29, -45.0),
    (19, 4.89, 0.67, 1.59),
    (20, 0.53, 8.12, 5.21))

  val zeroCorrelationFrame = sparkSession
    .createDataFrame(zeroCorrelationPoints)
    .selectExpr("_1 as id", "_2 AS x", "_3 AS y", "_4 AS value")
    .selectExpr("id", "ST_MakePoint(x, y) AS geometry", "value")

  val negativeCorrelationFrame = sparkSession
    .createDataFrame(negativeCorrelationPoints)
    .selectExpr("_1 as id", "_2 AS x", "_3 AS y", "_4 AS value")
    .selectExpr("id", "ST_MakePoint(x, y) AS geometry", "value")
}
