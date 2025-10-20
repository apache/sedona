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

import org.apache.sedona.common.sphere.Haversine
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.sql.utils.GeometrySerializer
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
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetFileFormatBase
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter.AndFilter
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter.LeafFilter
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter.OrFilter
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.{ST_AsEWKT, ST_Buffer, ST_Contains, ST_CoveredBy, ST_Covers, ST_Crosses, ST_DWithin, ST_Distance, ST_DistanceSphere, ST_DistanceSpheroid, ST_Equals, ST_Intersects, ST_OrderingEquals, ST_Overlaps, ST_Touches, ST_Within}
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils.splitConjunctivePredicates
import org.apache.spark.sql.types.DoubleType
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Point

class SpatialFilterPushDownForGeoParquet(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val enableSpatialFilterPushDown =
      sparkSession.conf.get("spark.sedona.geoparquet.spatialFilterPushDown", "true").toBoolean
    if (!enableSpatialFilterPushDown) plan
    else {
      plan transform {
        case filter @ Filter(condition, lr: LogicalRelation) if isGeoParquetRelation(lr) =>
          val filters = splitConjunctivePredicates(condition)
          val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, lr.output)
          val (_, normalizedFiltersWithoutSubquery) =
            normalizedFilters.partition(SubqueryExpression.hasSubquery)
          val geoParquetSpatialFilters =
            translateToGeoParquetSpatialFilters(normalizedFiltersWithoutSubquery)
          val hadoopFsRelation = lr.relation.asInstanceOf[HadoopFsRelation]
          val fileFormat = hadoopFsRelation.fileFormat.asInstanceOf[GeoParquetFileFormatBase]
          if (geoParquetSpatialFilters.isEmpty) filter
          else {
            val combinedSpatialFilter = geoParquetSpatialFilters.reduce(AndFilter)
            val newFileFormat = fileFormat.withSpatialPredicates(combinedSpatialFilter)
            val newRelation = hadoopFsRelation.copy(fileFormat = newFileFormat)(sparkSession)
            filter.copy(child = lr.copy(relation = newRelation))
          }
      }
    }
  }

  private def isGeoParquetRelation(lr: LogicalRelation): Boolean =
    lr.relation.isInstanceOf[HadoopFsRelation] &&
      lr.relation.asInstanceOf[HadoopFsRelation].fileFormat.isInstanceOf[GeoParquetFileFormatBase]

  def translateToGeoParquetSpatialFilters(
      predicates: Seq[Expression]): Seq[GeoParquetSpatialFilter] = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled = false)
    predicates.flatMap { predicate =>
      translateToGeoParquetSpatialFilter(predicate, pushableColumn)
    }
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
          yield LeafFilter(
            unquote(name),
            SpatialPredicate.INTERSECTS,
            GeometryUDT.deserialize(value))

      case LessThan(ST_Distance(distArgs), Literal(d, DoubleType)) =>
        for ((name, value) <- resolveNameAndLiteral(distArgs, pushableColumn))
          yield distanceFilter(name, GeometryUDT.deserialize(value), d.asInstanceOf[Double])

      case LessThanOrEqual(ST_Distance(distArgs), Literal(d, DoubleType)) =>
        for ((name, value) <- resolveNameAndLiteral(distArgs, pushableColumn))
          yield distanceFilter(name, GeometryUDT.deserialize(value), d.asInstanceOf[Double])

      case LessThan(ST_DistanceSpheroid(distArgs), Literal(d, DoubleType)) =>
        for ((name, value) <- resolveNameAndLiteral(distArgs, pushableColumn))
          yield distanceFilter(
            name,
            GeometryUDT.deserialize(value),
            d.asInstanceOf[Double],
            distanceType = "spheroid")

      case LessThanOrEqual(ST_DistanceSpheroid(distArgs), Literal(d, DoubleType)) =>
        for ((name, value) <- resolveNameAndLiteral(distArgs, pushableColumn))
          yield distanceFilter(
            name,
            GeometryUDT.deserialize(value),
            d.asInstanceOf[Double],
            distanceType = "spheroid")

      case LessThan(ST_DistanceSphere(distArgs), Literal(d, DoubleType)) =>
        val radiusOpt = distArgs.lift(2).collect {
          case Literal(customRadius: Double, DoubleType) => customRadius
        }

        resolveNameAndLiteral(distArgs.take(2), pushableColumn).map { case (name, value) =>
          distanceFilter(
            name,
            GeometryUDT.deserialize(value),
            d.asInstanceOf[Double],
            distanceType = "sphere",
            sphereRadiusOverride = radiusOpt)
        }

      case LessThanOrEqual(ST_DistanceSphere(distArgs), Literal(d, DoubleType)) =>
        val radiusOpt = distArgs.lift(2).collect {
          case Literal(customRadius: Double, DoubleType) => customRadius
        }

        resolveNameAndLiteral(distArgs.take(2), pushableColumn).map { case (name, value) =>
          distanceFilter(
            name,
            GeometryUDT.deserialize(value),
            d.asInstanceOf[Double],
            distanceType = "sphere",
            sphereRadiusOverride = radiusOpt)
        }

      case ST_DWithin(args) if args.length == 3 || args.length == 4 =>
        val distanceLit = args(2)
        val distanceType = args.lift(3) match {
          case Some(Literal(flag: Boolean, _)) => if (flag) "spheroid" else "planar"
          case _ => "planar"
        }

        distanceLit match {
          case Literal(distance: Double, DoubleType) =>
            resolveNameAndLiteral(args.take(2), pushableColumn).map { case (name, value) =>
              distanceFilter(name, GeometryUDT.deserialize(value), distance, distanceType)
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def distanceFilter(
      name: String,
      geom: Geometry,
      distance: Double,
      distanceType: String = "planar",
      sphereRadiusOverride: Option[Double] = None): GeoParquetSpatialFilter = {
    val queryWindow: Geometry = distanceType match {
      case "spheroid" =>
        // Spheroidal buffer
        // Increase buffer distance by 3% to account for false negatives with Spheroidal Buffer calculations
        val distanceLit = Literal(distance * 1.03)
        val spheroidLit = Literal(true)
        val geomLit = Literal.create(GeometrySerializer.serialize(geom), new GeometryUDT())

        val bufferGeometry = {
          val bufferExpr = ST_Buffer(
            scala.collection.immutable.Seq(geomLit, distanceLit, spheroidLit))
          val wkb = bufferExpr.eval().asInstanceOf[Array[Byte]]
          GeometrySerializer.deserialize(wkb)
        }
        bufferGeometry

      case "sphere" =>
        // The Haversine expandEnvelope already conservatively expands envelope by 10% to avoid false negatives
        val radius = sphereRadiusOverride.getOrElse(Haversine.AVG_EARTH_RADIUS)
        val expandedEnvelope =
          Haversine.expandEnvelope(geom.getEnvelopeInternal, distance, radius)
        geom.getFactory.toGeometry(expandedEnvelope)

      case _ =>
        // Euclidean distance
        val envelope = geom.getEnvelopeInternal
        envelope.expandBy(distance)
        geom.getFactory.toGeometry(envelope)
    }
    LeafFilter(unquote(name), SpatialPredicate.INTERSECTS, queryWindow)
  }

  private def unquote(name: String): String = {
    parseColumnPath(name).mkString(".")
  }

  private def resolveNameAndLiteral(
      expressions: Seq[Expression],
      pushableColumn: PushableColumnBase): Option[(String, Any)] = {
    expressions match {
      case Seq(pushableColumn(name), Literal(v, _)) => Some(name, v)
      case Seq(Literal(v, _), pushableColumn(name)) => Some(name, v)
      case _ => None
    }
  }
}
