package org.apache.spark.sql.sedona_sql.io

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

private[io] class ImageOptions(@transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {
  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))
  /**
   * Whether to drop invalid images. If true, invalid images will be removed, otherwise
   * invalid images will be returned with empty data and all other field filled with `-1`.
   */
  val dropInvalid = parameters.getOrElse("dropInvalid", "false").toBoolean
}