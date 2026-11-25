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
package org.apache.spark.sql.execution.datasources.geoparquet

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.sedona.common.S2Geography.Geography
import org.apache.sedona.common.geometryObjects.Box2D
import org.apache.sedona.common.sphere.Haversine
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.sql.utils.GeographySerializer
import org.apache.spark.sql.sedona_sql.types.{GeographyBounds, GeographyWkbScanner, PublicWkbHeaderAdapter}
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry

import scala.util.Try

/**
 * Filters containing spatial predicates such as `ST_Within(geom, ST_GeomFromText(...))` will be
 * converted to [[GeoParquetSpatialFilter]] and get pushed down to [[GeoParquetFileFormat]] by
 * [[org.apache.spark.sql.sedona_sql.optimization.SpatialFilterPushDownForGeoParquet]].
 */
trait GeoParquetSpatialFilter {

  /**
   * File-level evaluation against GeoParquet column metadata. Used for cheap whole-file pruning
   * before reading row-group statistics. Filters that cannot soundly prune at the file metadata
   * level should return `true` here and emit their pruning predicate via [[toParquetFilter]].
   */
  def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean

  /**
   * Translate this spatial filter into a Parquet [[FilterPredicate]] that the Parquet reader can
   * evaluate against row-group statistics. Returns `None` if the filter cannot be expressed as a
   * Parquet predicate (e.g. arbitrary JTS predicates on a geometry column).
   */
  def toParquetFilter: Option[FilterPredicate] = None

  /**
   * Whether a row group may have a row that this filter keeps. A filter that reads nothing of a
   * row group keeps it.
   *
   * @param geographyBox
   *   the bounding box that the geospatial statistics of the row group have for a GEOGRAPHY
   *   column with spherical edges: its western, eastern, southern and northern bound in degrees,
   *   with the western bound above the eastern bound across the antimeridian. None for a column
   *   without such a box.
   */
  def evaluateRowGroup(geographyBox: String => Option[Array[Double]]): Boolean = true

  /** Whether [[evaluateRowGroup]] reads the statistics of a row group at all. */
  def usesRowGroupStatistics: Boolean = false

  def simpleString: String
}

object GeoParquetSpatialFilter {

  case class AndFilter(left: GeoParquetSpatialFilter, right: GeoParquetSpatialFilter)
      extends GeoParquetSpatialFilter {
    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean = {
      left.evaluate(columns) && right.evaluate(columns)
    }

    override def toParquetFilter: Option[FilterPredicate] =
      (left.toParquetFilter, right.toParquetFilter) match {
        case (Some(l), Some(r)) => Some(FilterApi.and(l, r))
        case (Some(l), None) => Some(l)
        case (None, Some(r)) => Some(r)
        case _ => None
      }

    override def evaluateRowGroup(geographyBox: String => Option[Array[Double]]): Boolean =
      left.evaluateRowGroup(geographyBox) && right.evaluateRowGroup(geographyBox)

    override def usesRowGroupStatistics: Boolean =
      left.usesRowGroupStatistics || right.usesRowGroupStatistics

    override def simpleString: String = s"(${left.simpleString}) AND (${right.simpleString})"
  }

  case class OrFilter(left: GeoParquetSpatialFilter, right: GeoParquetSpatialFilter)
      extends GeoParquetSpatialFilter {
    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean =
      left.evaluate(columns) || right.evaluate(columns)

    // OR pushdown to Parquet requires both sides translate; otherwise we'd drop matching rows.
    override def toParquetFilter: Option[FilterPredicate] =
      for {
        l <- left.toParquetFilter
        r <- right.toParquetFilter
      } yield FilterApi.or(l, r)

    override def evaluateRowGroup(geographyBox: String => Option[Array[Double]]): Boolean =
      left.evaluateRowGroup(geographyBox) || right.evaluateRowGroup(geographyBox)

    override def usesRowGroupStatistics: Boolean =
      left.usesRowGroupStatistics || right.usesRowGroupStatistics

    override def simpleString: String = s"(${left.simpleString}) OR (${right.simpleString})"
  }

  /**
   * Spatial predicate pushed down to GeoParquet data source. We'll use the bbox in column
   * metadata to prune unrelated files.
   *
   * @param columnName
   *   name of filtered geometry column
   * @param predicateType
   *   type of spatial predicate, should be one of COVERS and INTERSECTS
   * @param queryWindow
   *   query window
   */
  case class LeafFilter(
      columnName: String,
      predicateType: SpatialPredicate,
      queryWindow: Geometry)
      extends GeoParquetSpatialFilter {
    def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean = {
      columns.get(columnName).forall { column =>
        val bbox = column.bbox.getOrElse(return true)
        // A bbox lists the minimum of every dimension and then the maximum of every dimension:
        // four values for XY, six for XYZ and eight for XYZM. X and Y come first in each half.
        // A file whose bbox has another number of values is kept.
        if (bbox.length != 4 && bbox.length != 6 && bbox.length != 8) {
          return true
        }

        val dimensions = bbox.length / 2
        val (xmin, ymin, xmax, ymax) = (bbox(0), bbox(1), bbox(dimensions), bbox(dimensions + 1))
        // A bbox that crosses the antimeridian lists the west bound first, as RFC 7946 says, so
        // its xmin is greater than its xmax. An envelope cannot express that range, and the file
        // is kept. So is a file with any other bounds that are not in order, NaN included.
        if (!(xmin <= xmax && ymin <= ymax)) {
          return true
        }

        val columnEnvelope =
          queryWindow.getFactory.toGeometry(new Envelope(xmin, xmax, ymin, ymax))
        predicateType match {
          case SpatialPredicate.COVERS => columnEnvelope.covers(queryWindow)
          case SpatialPredicate.INTERSECTS =>
            // XXX: We must call the intersects method of queryWindow instead of columnEnvelope, since queryWindow
            // may be a Circle object and geom.intersects(circle) may not work correctly.
            queryWindow.intersects(columnEnvelope)
          case _ =>
            throw new IllegalArgumentException(s"Unexpected predicate type: $predicateType")
        }
      }
    }
    override def simpleString: String = s"$columnName ${predicateType.name} $queryWindow"
  }

  /**
   * A predicate between a geography column and a geography of the query that holds for no row
   * whose geography is outside of a box around the geography of the query. Two geographies that
   * intersect have a point in common, and so have one that contains the other and two that are
   * equal, so one box serves these predicates. For a predicate on the distance between the two,
   * the box is larger by that distance.
   *
   * The `geo` metadata of a file has no bounding box for a geography column, so no file is
   * skipped by its metadata. A row group is skipped when the bounding box in its geospatial
   * statistics and the box of the query have no point in common.
   *
   * @param columnName
   *   name of the filtered geography column
   * @param west
   *   western bound of the box of the query in degrees, above `east` across the antimeridian
   */
  case class GeographyLeafFilter(
      columnName: String,
      west: Double,
      east: Double,
      south: Double,
      north: Double)
      extends GeoParquetSpatialFilter {
    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean = true

    override def evaluateRowGroup(geographyBox: String => Option[Array[Double]]): Boolean =
      geographyBox(columnName).forall(
        GeographyBounds.intersects(_, Array(west, east, south, north)))

    override def usesRowGroupStatistics: Boolean = true

    override def simpleString: String =
      s"$columnName INTERSECTS GEOGRAPHY BOX($west $south, $east $north)"
  }

  object GeographyLeafFilter {

    /**
     * The filter for the rows of `columnName` whose geography is within `meters` of `query`, on a
     * sphere of the radius that Sedona measures the distance between geographies on. None for a
     * query that has no box: an empty one, one with a coordinate that is no longitude or no
     * latitude, and one that cannot be read.
     */
    def within(
        columnName: String,
        query: Geography,
        meters: Double): Option[GeographyLeafFilter] =
      if (!(meters >= 0) || meters.isInfinity) None
      else
        Try {
          var bytes = GeographySerializer.serialize(query)
          var offset = GeographySerializer.WKB_OFFSET
          val scanned = GeographyWkbScanner.scan(bytes, offset, true, columnName)
          if ((scanned & GeographyWkbScanner.NOT_ISO) != 0) {
            bytes = PublicWkbHeaderAdapter.toIsoWkbInPlace(
              java.util.Arrays.copyOfRange(bytes, offset, bytes.length))
            offset = 0
          }
          val bounds = new GeographyBounds
          bounds.add(bytes, offset)
          Option(bounds.box()).map { box =>
            // A little more than the distance, for the rounding of the distance of a row
            val grown =
              if (meters == 0) box
              else GeographyBounds.expand(box, meters / Haversine.AVG_EARTH_RADIUS * (1 + 1e-9))
            GeographyLeafFilter(columnName, grown(0), grown(1), grown(2), grown(3))
          }
        }.toOption.flatten
  }

  /**
   * Semantic kind of a Box2D leaf predicate. Determines which inequality system is emitted as a
   * Parquet filter against the four (xmin, ymin, xmax, ymax) leaf columns of a Box2D-typed
   * column.
   */
  sealed trait Box2DPredicateKind {
    def simpleName: String
  }
  object Box2DPredicateKind {

    /** `ST_Intersects(box_col, lit)` — symmetric, same regardless of argument order. */
    case object Intersects extends Box2DPredicateKind {
      override def simpleName: String = "INTERSECTS"
    }

    /** `ST_Contains(box_col, lit)` — the column box must contain the literal box. */
    case object ColumnContainsLiteral extends Box2DPredicateKind {
      override def simpleName: String = "CONTAINS"
    }

    /** `ST_Contains(lit, box_col)` — the literal box must contain the column box. */
    case object LiteralContainsColumn extends Box2DPredicateKind {
      override def simpleName: String = "CONTAINED_BY"
    }
  }

  /**
   * Pushdown filter for predicates that operate on a Box2D-typed column (e.g.
   * `ST_Intersects(box_col, lit_box)` or `ST_Contains(box_col, lit_box)`).
   *
   * Pruning is performed by translating the predicate into per-leaf inequalities on the Box2D
   * column's four `Double` fields (`xmin`, `ymin`, `xmax`, `ymax`) and pushing the result down as
   * a Parquet [[FilterPredicate]]. Parquet's row-group statistics machinery then skips row groups
   * whose per-column min/max bounds disprove the predicate.
   *
   * File-metadata evaluation returns `true` (i.e. don't prune at the GeoParquet metadata layer)
   * because that path relied on the geometry column's bbox and is unsound when the GeoParquet 1.1
   * spec permits coverings to be conservatively wider than per-row envelopes. The Parquet-stats
   * path uses the Box2D column's actual recorded min/max, so it is sound for any writer.
   */
  case class Box2DLeafFilter(
      box2dColumnName: String,
      predicateKind: Box2DPredicateKind,
      queryBox: Box2D)
      extends GeoParquetSpatialFilter {

    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean = true

    override def toParquetFilter: Option[FilterPredicate] = {
      val xmin = FilterApi.doubleColumn(s"$box2dColumnName.xmin")
      val ymin = FilterApi.doubleColumn(s"$box2dColumnName.ymin")
      val xmax = FilterApi.doubleColumn(s"$box2dColumnName.xmax")
      val ymax = FilterApi.doubleColumn(s"$box2dColumnName.ymax")
      val qxMin = java.lang.Double.valueOf(queryBox.getXMin)
      val qyMin = java.lang.Double.valueOf(queryBox.getYMin)
      val qxMax = java.lang.Double.valueOf(queryBox.getXMax)
      val qyMax = java.lang.Double.valueOf(queryBox.getYMax)

      val predicate = predicateKind match {
        case Box2DPredicateKind.Intersects =>
          // Intersection: row's xmax >= lit.xmin && xmin <= lit.xmax && ymax >= lit.ymin && ymin <= lit.ymax
          FilterApi.and(
            FilterApi.and(FilterApi.gtEq(xmax, qxMin), FilterApi.ltEq(xmin, qxMax)),
            FilterApi.and(FilterApi.gtEq(ymax, qyMin), FilterApi.ltEq(ymin, qyMax)))
        case Box2DPredicateKind.ColumnContainsLiteral =>
          // Column contains literal: row's xmin <= lit.xmin && xmax >= lit.xmax && ymin <= lit.ymin && ymax >= lit.ymax
          FilterApi.and(
            FilterApi.and(FilterApi.ltEq(xmin, qxMin), FilterApi.gtEq(xmax, qxMax)),
            FilterApi.and(FilterApi.ltEq(ymin, qyMin), FilterApi.gtEq(ymax, qyMax)))
        case Box2DPredicateKind.LiteralContainsColumn =>
          // Literal contains column: row's xmin >= lit.xmin && xmax <= lit.xmax && ymin >= lit.ymin && ymax <= lit.ymax
          FilterApi.and(
            FilterApi.and(FilterApi.gtEq(xmin, qxMin), FilterApi.ltEq(xmax, qxMax)),
            FilterApi.and(FilterApi.gtEq(ymin, qyMin), FilterApi.ltEq(ymax, qyMax)))
      }
      Some(predicate)
    }

    override def simpleString: String =
      s"$box2dColumnName ${predicateKind.simpleName} BOX(${queryBox.getXMin} ${queryBox.getYMin}, " +
        s"${queryBox.getXMax} ${queryBox.getYMax})"
  }
}
