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
          // A limit is safe only when it is applied directly to this scan. Filters, sorts,
          // aggregates, joins, and other intervening operators can reject or reorder rows.
          scan.limit = extractDirectLimit(plan, lr)
          lr
      }
    }
  }

  private[optimization] def extractDirectLimit(
      plan: LogicalPlan,
      target: LogicalPlan): Option[Int] = {
    def literalLimit(expression: Expression): Option[Int] = expression match {
      case Literal(limit: Int, _) => Some(limit)
      case _ => None
    }

    plan match {
      case GlobalLimit(globalExpression, LocalLimit(localExpression, child))
          if child.eq(target) =>
        for {
          globalLimit <- literalLimit(globalExpression)
          localLimit <- literalLimit(localExpression)
          if localLimit >= globalLimit
        } yield globalLimit
      case GlobalLimit(expression, child) if child.eq(target) =>
        literalLimit(expression)
      case _ => None
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

      case LessThan(pushableColumn(name), Literal(v, TimestampType))
          if isPushableTemporalColumn(name) =>
        Some(
          TemporalFilter
            .LessThanFilter(unquote(name), microsToLocalDateTime(v.asInstanceOf[Long])))

      case LessThanOrEqual(pushableColumn(name), Literal(v, TimestampType))
          if isPushableTemporalColumn(name) =>
        Some(
          TemporalFilter
            .LessThanFilter(unquote(name), microsToLocalDateTime(v.asInstanceOf[Long])))

      case GreaterThan(pushableColumn(name), Literal(v, TimestampType))
          if isPushableTemporalColumn(name) =>
        Some(
          TemporalFilter
            .GreaterThanFilter(unquote(name), microsToLocalDateTime(v.asInstanceOf[Long])))

      case GreaterThanOrEqual(pushableColumn(name), Literal(v, TimestampType))
          if isPushableTemporalColumn(name) =>
        Some(
          TemporalFilter
            .GreaterThanFilter(unquote(name), microsToLocalDateTime(v.asInstanceOf[Long])))

      case EqualTo(pushableColumn(name), Literal(v, TimestampType))
          if isPushableTemporalColumn(name) =>
        Some(
          TemporalFilter
            .EqualFilter(unquote(name), microsToLocalDateTime(v.asInstanceOf[Long])))

      case _ => None
    }
  }

  /**
   * Converts a Spark TimestampType literal (microseconds since the Unix epoch) to a
   * [[LocalDateTime]] in UTC without losing sub-millisecond precision. The previous
   * implementation divided by 1000 to obtain milliseconds, which truncated the pushed-down
   * temporal bound to millisecond resolution and could drop items in the final fraction of a
   * second before Spark's residual filter ran. `floorDiv`/`floorMod` keep the conversion correct
   * for pre-epoch timestamps as well.
   */
  private def microsToLocalDateTime(micros: Long): LocalDateTime = {
    val seconds = Math.floorDiv(micros, 1000000L)
    val nanoOfSecond = Math.floorMod(micros, 1000000L) * 1000L
    LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanoOfSecond), ZoneOffset.UTC)
  }

  private def unquote(name: String): String = {
    parseColumnPath(name).mkString(".")
  }

  private def isPushableTemporalColumn(name: String): Boolean = unquote(name) == "datetime"
}
