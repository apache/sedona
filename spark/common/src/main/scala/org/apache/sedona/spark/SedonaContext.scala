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
package org.apache.sedona.spark

import org.apache.sedona.common.utils.TelemetryCollector
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.RasterRegistrator
import org.apache.sedona.sql.UDF.Catalog
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.sedona_sql.optimization._
import org.apache.spark.sql.sedona_sql.strategy.join.JoinQueryDetector
import org.apache.spark.sql.sedona_sql.strategy.physical.function.EvalPhysicalFunctionStrategy
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.annotation.StaticAnnotation
import scala.util.Try

class InternalApi(
    description: String = "This method is for internal use only and may change without notice.")
    extends StaticAnnotation

object SedonaContext {

  private def customOptimizationsWithSession(sparkSession: SparkSession) =
    Seq(
      new SpatialFilterPushDownForGeoParquet(sparkSession),
      new SpatialTemporalFilterPushDownForStacScan(sparkSession))

  def create(sqlContext: SQLContext): SQLContext = {
    create(sqlContext.sparkSession)
    sqlContext
  }

  /**
   * This is the entry point of the entire Sedona system
   *
   * @param sparkSession
   * @return
   */
  def create(sparkSession: SparkSession): SparkSession = {
    create(sparkSession, "java")
  }

  @InternalApi
  def create(sparkSession: SparkSession, language: String): SparkSession = {
    TelemetryCollector.send("spark", language)
    if (!sparkSession.experimental.extraStrategies.exists(_.isInstanceOf[JoinQueryDetector])) {
      sparkSession.experimental.extraStrategies ++= Seq(new JoinQueryDetector(sparkSession))
    }

    val sedonaArrowStrategy = Try(
      Class
        .forName("org.apache.spark.sql.udf.SedonaArrowStrategy")
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[SparkStrategy])

    val extractSedonaUDFRule =
      Try(
        Class
          .forName("org.apache.spark.sql.udf.ExtractSedonaUDFRule")
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[Rule[LogicalPlan]])

    if (sedonaArrowStrategy.isSuccess && extractSedonaUDFRule.isSuccess) {
      sparkSession.experimental.extraStrategies =
        sparkSession.experimental.extraStrategies :+ sedonaArrowStrategy.get
      sparkSession.experimental.extraOptimizations =
        sparkSession.experimental.extraOptimizations :+ extractSedonaUDFRule.get
    }

    customOptimizationsWithSession(sparkSession).foreach { opt =>
      if (!sparkSession.experimental.extraOptimizations.exists {
          case _: opt.type => true
          case _ => false
        }) {
        sparkSession.experimental.extraOptimizations ++= Seq(opt)
      }
    }

    // Support physical functions
    if (!sparkSession.experimental.extraOptimizations.contains(ExtractPhysicalFunctions)) {
      sparkSession.experimental.extraOptimizations ++= Seq(ExtractPhysicalFunctions)
    }

    if (!sparkSession.experimental.extraStrategies.exists(
        _.isInstanceOf[EvalPhysicalFunctionStrategy])) {
      sparkSession.experimental.extraStrategies ++= Seq(
        new EvalPhysicalFunctionStrategy(sparkSession))
    }

    addGeoParquetToSupportNestedFilterSources(sparkSession)
    RasterRegistrator.registerAll(sparkSession)
    UdtRegistrator.registerAll()
    Catalog.registerAll(sparkSession)
    sparkSession
  }

  /**
   * This method adds the basic Sedona configurations to the SparkSession Usually the user does
   * not need to call this method directly This is only needed when the user needs to manually
   * configure Sedona
   *
   * @return
   */
  def builder(): SparkSession.Builder = {
    SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
  }

  private def addGeoParquetToSupportNestedFilterSources(session: SparkSession): Unit = {
    // File formats that support nested predicate pushdown is configured by
    // spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources, which is a comma-separated list of data source
    // names. We need to append "geoparquet" to the list to enable nested predicate pushdown for GeoParquet.
    val sources =
      Try(session.conf.get("spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources"))
        .getOrElse("")
    if (!sources.contains("geoparquet")) {
      val newSources = if (sources.isEmpty) "geoparquet" else sources + ",geoparquet"
      session.conf.set(
        "spark.sql.optimizer.nestedPredicatePushdown.supportedFileSources",
        newSources)
    }
  }
}
