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
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Or, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LocalLimit, LogicalPlan}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter.{AndFilter => TemporalAndFilter}
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter.{AndFilter => SpatialAndFilter}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, PushableColumn, PushableColumnBase}
import org.apache.spark.sql.sedona_sql.io.stac.StacScan
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils.splitConjunctivePredicates
import org.apache.spark.sql.types.TimestampType

import java.time.{Instant, LocalDateTime, ZoneOffset}

/*
 * This class is responsible for pushing down spatial filters to the STAC data source.
 * It extends and reuses the `SpatialFilterPushDownForGeoParquet` class, which is responsible for pushing down
 */
class SpatialTemporalFilterPushDownForStacScan(sparkSession: SparkSession)
    extends SpatialFilterPushDownForGeoParquet(sparkSession) {

  /**
   * Pushes down spatial filters to the STAC data source.
   *
   * @param plan
   *   The logical plan to optimize.
   * @return
   *   The optimized logical plan with spatial filters pushed down to the STAC data source.
   */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val enableSpatialFilterPushDown =
      sparkSession.conf.get("spark.sedona.stac.spatialFilterPushDown", "true").toBoolean
    if (!enableSpatialFilterPushDown) plan
    else {
      plan transform {
        case filter @ Filter(condition, lr: DataSourceV2ScanRelation) if isStacScanRelation(lr) =>
          val filters = splitConjunctivePredicates(condition)
          val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, lr.output)
          val (_, normalizedFiltersWithoutSubquery) =
            normalizedFilters.partition(SubqueryExpression.hasSubquery)
          // reuse the `translateToGeoParquetSpatialFilters` method from the `SpatialFilterPushDownForGeoParquet` class
          val spatialFilters =
            translateToGeoParquetSpatialFilters(normalizedFiltersWithoutSubquery)
          if (!spatialFilters.isEmpty) {
            val combinedSpatialFilter = spatialFilters.reduce(SpatialAndFilter)
            val scan = lr.scan.asInstanceOf[StacScan]
            // set the spatial predicates in the STAC scan
            scan.setSpatialPredicates(combinedSpatialFilter)
            filter.copy()
          }
          val temporalFilters =
            translateToTemporalFilters(normalizedFiltersWithoutSubquery)
          if (!temporalFilters.isEmpty) {
            val combinedTemporalFilter = temporalFilters.reduce(TemporalAndFilter)
            val scan = lr.scan.asInstanceOf[StacScan]
            // set the spatial predicates in the STAC scan
            scan.setTemporalPredicates(combinedTemporalFilter)
            filter.copy()
          }
          filter.copy()
        case lr: DataSourceV2ScanRelation if isStacScanRelation(lr) =>
          val scan = lr.scan.asInstanceOf[StacScan]
          val limit = extractLimit(plan)
          limit match {
            case Some(n) => scan.setLimit(n)
            case None =>
          }
          lr
      }
    }
  }

  def extractLimit(plan: LogicalPlan): Option[Int] = {
    plan.collectFirst {
      case GlobalLimit(Literal(limit: Int, _), _) => limit
      case LocalLimit(Literal(limit: Int, _), _) => limit
    }
  }

  private def isStacScanRelation(lr: DataSourceV2ScanRelation): Boolean =
    lr.scan.isInstanceOf[StacScan]

  def translateToTemporalFilters(predicates: Seq[Expression]): Seq[TemporalFilter] = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled = true)
    predicates.flatMap { predicate =>
      translateToTemporalFilter(predicate, pushableColumn)
    }
  }

  private def translateToTemporalFilter(
      predicate: Expression,
      pushableColumn: PushableColumnBase): Option[TemporalFilter] = {
    predicate match {
      case And(left, right) =>
        val temporalFilterLeft = translateToTemporalFilter(left, pushableColumn)
        val temporalFilterRight = translateToTemporalFilter(right, pushableColumn)
        (temporalFilterLeft, temporalFilterRight) match {
          case (Some(l), Some(r)) => Some(TemporalFilter.AndFilter(l, r))
          case (Some(l), None) => Some(l)
          case (None, Some(r)) => Some(r)
          case _ => None
        }

      case Or(left, right) =>
        for {
          temporalFilterLeft <- translateToTemporalFilter(left, pushableColumn)
          temporalFilterRight <- translateToTemporalFilter(right, pushableColumn)
        } yield TemporalFilter.OrFilter(temporalFilterLeft, temporalFilterRight)

      case LessThan(pushableColumn(name), Literal(v, TimestampType)) =>
        Some(
          TemporalFilter
            .LessThanFilter(
              unquote(name),
              LocalDateTime
                .ofInstant(Instant.ofEpochMilli(v.asInstanceOf[Long] / 1000), ZoneOffset.UTC)))

      case LessThanOrEqual(pushableColumn(name), Literal(v, TimestampType)) =>
        Some(
          TemporalFilter
            .LessThanFilter(
              unquote(name),
              LocalDateTime
                .ofInstant(Instant.ofEpochMilli(v.asInstanceOf[Long] / 1000), ZoneOffset.UTC)))

      case GreaterThan(pushableColumn(name), Literal(v, TimestampType)) =>
        Some(
          TemporalFilter
            .GreaterThanFilter(
              unquote(name),
              LocalDateTime
                .ofInstant(Instant.ofEpochMilli(v.asInstanceOf[Long] / 1000), ZoneOffset.UTC)))

      case GreaterThanOrEqual(pushableColumn(name), Literal(v, TimestampType)) =>
        Some(
          TemporalFilter
            .GreaterThanFilter(
              unquote(name),
              LocalDateTime
                .ofInstant(Instant.ofEpochMilli(v.asInstanceOf[Long] / 1000), ZoneOffset.UTC)))

      case EqualTo(pushableColumn(name), Literal(v, TimestampType)) =>
        Some(
          TemporalFilter
            .EqualFilter(
              unquote(name),
              LocalDateTime
                .ofInstant(Instant.ofEpochMilli(v.asInstanceOf[Long] / 1000), ZoneOffset.UTC)))

      case _ => None
    }
  }

  private def unquote(name: String): String = {
    parseColumnPath(name).mkString(".")
  }
}
