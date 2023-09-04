/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry

/**
 * Filters containing spatial predicates such as `ST_Within(geom, ST_GeomFromText(...))` will be converted
 * to [[GeoParquetSpatialFilter]] and get pushed down to [[GeoParquetFileFormat]] by
 * [[org.apache.spark.sql.sedona_sql.optimization.SpatialFilterPushDownForGeoParquet]].
 */
trait GeoParquetSpatialFilter {
  def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean
}

object GeoParquetSpatialFilter {

  case class AndFilter(left: GeoParquetSpatialFilter, right: GeoParquetSpatialFilter) extends GeoParquetSpatialFilter {
    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean =
      left.evaluate(columns) && right.evaluate(columns)
  }

  case class OrFilter(left: GeoParquetSpatialFilter, right: GeoParquetSpatialFilter) extends GeoParquetSpatialFilter {
    override def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean =
      left.evaluate(columns) || right.evaluate(columns)
  }

  /**
   * Spatial predicate pushed down to GeoParquet data source. We'll use the bbox in column metadata to prune
   * unrelated files.
   *
   * @param columnName    name of filtered geometry column
   * @param predicateType type of spatial predicate, should be one of COVERS and INTERSECTS
   * @param queryWindow   query window
   */
  case class LeafFilter(
    columnName: String,
    predicateType: SpatialPredicate,
    queryWindow: Geometry) extends GeoParquetSpatialFilter {
    def evaluate(columns: Map[String, GeometryFieldMetaData]): Boolean = {
      columns.get(columnName).forall { column =>
        val bbox = column.bbox
        val columnEnvelope = queryWindow.getFactory.toGeometry(new Envelope(bbox(0), bbox(2), bbox(1), bbox(3)))
        predicateType match {
          case SpatialPredicate.COVERS => columnEnvelope.covers(queryWindow)
          case SpatialPredicate.INTERSECTS =>
            // XXX: We must call the intersects method of queryWindow instead of columnEnvelope, since queryWindow
            // may be a Circle object and geom.intersects(circle) may not work correctly.
            queryWindow.intersects(columnEnvelope)
          case _ => throw new IllegalArgumentException(s"Unexpected predicate type: $predicateType")
        }
      }
    }
  }
}
