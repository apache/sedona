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

import org.apache.sedona.common.geometryObjects.Circle
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.PushableColumn
import org.apache.spark.sql.execution.datasources.PushableColumnBase
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetFileFormatBase
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetSpatialFilter
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetSpatialFilter.AndFilter
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetSpatialFilter.LeafFilter
import org.apache.spark.sql.execution.datasources.parquet.GeoParquetSpatialFilter.OrFilter
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.ST_Contains
import org.apache.spark.sql.sedona_sql.expressions.ST_CoveredBy
import org.apache.spark.sql.sedona_sql.expressions.ST_Covers
import org.apache.spark.sql.sedona_sql.expressions.ST_Crosses
import org.apache.spark.sql.sedona_sql.expressions.ST_Distance
import org.apache.spark.sql.sedona_sql.expressions.ST_Equals
import org.apache.spark.sql.sedona_sql.expressions.ST_Intersects
import org.apache.spark.sql.sedona_sql.expressions.ST_OrderingEquals
import org.apache.spark.sql.sedona_sql.expressions.ST_Overlaps
import org.apache.spark.sql.sedona_sql.expressions.ST_Touches
import org.apache.spark.sql.sedona_sql.expressions.ST_Within
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils.splitConjunctivePredicates
import org.apache.spark.sql.types.DoubleType
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Point

class SpatialFilterPushDownForGeoParquet(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter@Filter(condition, lr: LogicalRelation) if isGeoParquetRelation(lr) =>
      val filters = splitConjunctivePredicates(condition)
      val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, lr.output)
      val (_, normalizedFiltersWithoutSubquery) = normalizedFilters.partition(SubqueryExpression.hasSubquery)
      val geoParquetSpatialFilters = translateToGeoParquetSpatialFilters(normalizedFiltersWithoutSubquery)
      val hadoopFsRelation = lr.relation.asInstanceOf[HadoopFsRelation]
      val fileFormat = hadoopFsRelation.fileFormat.asInstanceOf[GeoParquetFileFormatBase]
      if (geoParquetSpatialFilters.isEmpty) filter else {
        val combinedSpatialFilter = geoParquetSpatialFilters.reduce(AndFilter)
        val newFileFormat = fileFormat.withSpatialPredicates(combinedSpatialFilter)
        val newRelation = hadoopFsRelation.copy(fileFormat = newFileFormat)(sparkSession)
        filter.copy(child = lr.copy(relation = newRelation))
      }
  }

  private def isGeoParquetRelation(lr: LogicalRelation): Boolean =
    lr.relation.isInstanceOf[HadoopFsRelation] &&
      lr.relation.asInstanceOf[HadoopFsRelation].fileFormat.isInstanceOf[GeoParquetFileFormatBase]

  private def translateToGeoParquetSpatialFilters(predicates: Seq[Expression]): Seq[GeoParquetSpatialFilter] = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled = false)
    predicates.flatMap { predicate => translateToGeoParquetSpatialFilter(predicate, pushableColumn) }
  }

  private def translateToGeoParquetSpatialFilter(
    predicate: Expression,
    pushableColumn: PushableColumnBase): Option[GeoParquetSpatialFilter] = {
    predicate match {
      case And(left, right) =>
        val spatialFilterLeft = translateToGeoParquetSpatialFilter(left, pushableColumn)
        val spatialFilterRight = translateToGeoParquetSpatialFilter(right, pushableColumn)
        (spatialFilterLeft, spatialFilterRight) match {
          case (Some(l), Some(r)) => Some(AndFilter(l, r))
          case (Some(l), None) => Some(l)
          case (None, Some(r)) => Some(r)
          case _ => None
        }

      case Or(left, right) =>
        for {
          spatialFilterLeft <- translateToGeoParquetSpatialFilter(left, pushableColumn)
          spatialFilterRight <- translateToGeoParquetSpatialFilter(right, pushableColumn)
        } yield OrFilter(spatialFilterLeft, spatialFilterRight)

      case Not(_) => None

      case ST_Contains(Seq(pushableColumn(name), Literal(v, _))) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.COVERS, GeometryUDT.deserialize(v)))
      case ST_Contains(Seq(Literal(v, _), pushableColumn(name))) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, GeometryUDT.deserialize(v)))

      case ST_Covers(Seq(pushableColumn(name), Literal(v, _))) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.COVERS, GeometryUDT.deserialize(v)))
      case ST_Covers(Seq(Literal(v, _), pushableColumn(name))) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, GeometryUDT.deserialize(v)))

      case ST_Within(Seq(pushableColumn(name), Literal(v, _))) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, GeometryUDT.deserialize(v)))
      case ST_Within(Seq(Literal(v, _), pushableColumn(name))) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.COVERS, GeometryUDT.deserialize(v)))

      case ST_CoveredBy(Seq(pushableColumn(name), Literal(v, _))) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, GeometryUDT.deserialize(v)))
      case ST_CoveredBy(Seq(Literal(v, _), pushableColumn(name))) =>
        Some(LeafFilter(unquote(name), SpatialPredicate.COVERS, GeometryUDT.deserialize(v)))

      case ST_Equals(_) | ST_OrderingEquals(_) =>
        for ((name, value) <- resolveNameAndLiteral(predicate.children, pushableColumn))
          yield LeafFilter(unquote(name), SpatialPredicate.COVERS, GeometryUDT.deserialize(value))

      case ST_Intersects(_) | ST_Crosses(_) | ST_Overlaps(_) | ST_Touches(_) =>
        for ((name, value) <- resolveNameAndLiteral(predicate.children, pushableColumn))
          yield LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, GeometryUDT.deserialize(value))

      case LessThan(ST_Distance(distArgs), Literal(d, DoubleType)) =>
        for ((name, value) <- resolveNameAndLiteral(distArgs, pushableColumn))
          yield distanceFilter(name, GeometryUDT.deserialize(value), d.asInstanceOf[Double])

      case LessThanOrEqual(ST_Distance(distArgs), Literal(d, DoubleType)) =>
        for ((name, value) <- resolveNameAndLiteral(distArgs, pushableColumn))
          yield distanceFilter(name, GeometryUDT.deserialize(value), d.asInstanceOf[Double])

      case _ => None
    }
  }

  private def distanceFilter(name: String, geom: Geometry, distance: Double) = {
    val queryWindow = geom match {
      case point: Point => new Circle(point, distance)
      case _ =>
        val envelope = geom.getEnvelopeInternal
        envelope.expandBy(distance)
        geom.getFactory.toGeometry(envelope)
    }
    LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, queryWindow)
  }

  private def unquote(name: String): String = {
    parseColumnPath(name).mkString(".")
  }

  private def resolveNameAndLiteral(expressions: Seq[Expression], pushableColumn: PushableColumnBase): Option[(String, Any)] = {
    expressions match {
      case Seq(pushableColumn(name), Literal(v, _)) => Some(name, v)
      case Seq(Literal(v, _), pushableColumn(name)) => Some(name, v)
      case _ => None
    }
  }
}
