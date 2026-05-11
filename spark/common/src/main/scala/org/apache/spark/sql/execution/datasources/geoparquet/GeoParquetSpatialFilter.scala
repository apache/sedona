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

import org.apache.sedona.common.geometryObjects.Box2D
import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry

/**
 * Filters containing spatial predicates such as `ST_Within(geom, ST_GeomFromText(...))` will be
 * converted to [[GeoParquetSpatialFilter]] and get pushed down to [[GeoParquetFileFormat]] by
 * [[org.apache.spark.sql.sedona_sql.optimization.SpatialFilterPushDownForGeoParquet]].
 */
trait GeoParquetSpatialFilter {
  def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean
  def simpleString: String
}

object GeoParquetSpatialFilter {

  case class AndFilter(left: GeoParquetSpatialFilter, right: GeoParquetSpatialFilter)
      extends GeoParquetSpatialFilter {
    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean = {
      left.evaluate(columns) && right.evaluate(columns)
    }

    override def simpleString: String = s"(${left.simpleString}) AND (${right.simpleString})"
  }

  case class OrFilter(left: GeoParquetSpatialFilter, right: GeoParquetSpatialFilter)
      extends GeoParquetSpatialFilter {
    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean =
      left.evaluate(columns) || right.evaluate(columns)
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
        if (bbox.isEmpty) {
          return true
        }

        val columnEnvelope =
          queryWindow.getFactory.toGeometry(new Envelope(bbox(0), bbox(2), bbox(1), bbox(3)))
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
   * Pushdown filter for predicates that operate on a Box2D-typed column (e.g.
   * `ST_BoxIntersects(box_col, lit_box)` or `ST_BoxContains(box_col, lit_box)`).
   *
   * Per-file evaluation: walks the file's GeoParquet column metadata to find the geometry column
   * whose covering metadata points at `box2dColumnName`, then prunes using that geometry column's
   * recorded bbox.
   *
   * Both intersects and contains map to a file-level INTERSECTS check: per-row containment
   * implies per-row intersection, which implies the file's union envelope must intersect the
   * query box for any row to match. If no geometry column references this Box2D column as its
   * covering, the file is kept (cannot prune safely).
   *
   * @param box2dColumnName
   *   the Box2D column referenced by the predicate
   * @param queryBox
   *   the literal Box2D from the predicate's RHS
   */
  case class Box2DLeafFilter(box2dColumnName: String, queryBox: Box2D)
      extends GeoParquetSpatialFilter {

    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean = {
      // Find the geometry column whose covering metadata points at this Box2D column.
      val matchingGeomEntry = columns.find { case (_, field) =>
        field.covering.exists(_.bbox.xmin.headOption.contains(box2dColumnName))
      }

      matchingGeomEntry match {
        case Some((_, field)) =>
          // Use the geometry column's recorded bbox to prune. The union of per-row Box2D values
          // is a superset of the geometry column's bbox (covering boxes are at least as wide as
          // their geometries), so if the geom-column bbox does not intersect the query box, no
          // row's Box2D can intersect either. May leave some files unpruned when Box2D values
          // are conservatively wider than geometries, but never produces false negatives.
          val bbox = field.bbox.getOrElse(return true)
          if (bbox.isEmpty) return true
          val fileXMin = bbox(0)
          val fileYMin = bbox(1)
          val fileXMax = bbox(2)
          val fileYMax = bbox(3)
          !(fileXMax < queryBox.getXMin || fileXMin > queryBox.getXMax
            || fileYMax < queryBox.getYMin || fileYMin > queryBox.getYMax)
        case None =>
          // No geometry column references this Box2D column as covering — cannot prune safely.
          true
      }
    }

    override def simpleString: String =
      s"$box2dColumnName INTERSECTS BOX(${queryBox.getXMin} ${queryBox.getYMin}, " +
        s"${queryBox.getXMax} ${queryBox.getYMax})"
  }
}
