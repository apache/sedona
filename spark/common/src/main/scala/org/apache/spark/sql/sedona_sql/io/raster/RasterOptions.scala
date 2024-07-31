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
package org.apache.spark.sql.sedona_sql.io.raster

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

private[io] class RasterOptions(@transient private val parameters: CaseInsensitiveMap[String])
    extends Serializable {
  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  // The file format of the raster image
  val fileExtension = parameters.getOrElse("fileExtension", ".tiff")
  // Column of the raster image name
  val rasterPathField = parameters.get("pathField")
  // Column of the raster image itself
  val rasterField = parameters.get("rasterField")
  // Use direct committer to directly write to the final destination
  val useDirectCommitter = parameters.getOrElse("useDirectCommitter", "true").toBoolean
}
