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

import org.apache.sedona.sql.UDF.RasterUdafCatalog
import org.apache.sedona.sql.utils.GeoToolsCoverageAvailability.{gridClassName, isGeoToolsAvailable}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.sedona_sql.UDT.RasterUdtRegistratorWrapper
import org.apache.spark.sql.{SparkSession, functions}
import org.slf4j.{Logger, LoggerFactory}

object RasterRegistrator {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def registerAll(sparkSession: SparkSession): Unit = {
    if (isGeoToolsAvailable) {
      RasterUdtRegistratorWrapper.registerAll(gridClassName)
      sparkSession.udf.register(
        RasterUdafCatalog.rasterAggregateExpression.getClass.getSimpleName,
        functions.udaf(RasterUdafCatalog.rasterAggregateExpression))
    }
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    if (isGeoToolsAvailable) {
      sparkSession.sessionState.functionRegistry.dropFunction(
        FunctionIdentifier(RasterUdafCatalog.rasterAggregateExpression.getClass.getSimpleName))
    }
  }
}
