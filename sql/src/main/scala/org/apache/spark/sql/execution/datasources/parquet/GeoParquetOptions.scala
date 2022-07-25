package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class GeoParquetOptions(@transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * geometry field name. Default: geometry
   */

  val fieldGeometry = parameters.getOrElse("fieldGeometry", "geometry")
}
