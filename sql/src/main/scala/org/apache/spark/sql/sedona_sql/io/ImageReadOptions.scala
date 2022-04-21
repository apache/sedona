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
package org.apache.spark.sql.sedona_sql.io

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

private[io] class ImageReadOptions(@transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {
  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))
  /**
   * Optional parameters for reading GeoTiff
   * dropInvalid indicatesWhether to drop invalid images. If true, invalid images will be removed, otherwise
   * invalid images will be returned with empty data and all other field filled with `-1`.
   * disableErrorInCRS indicates whether to disable to errors in CRS transformation
   * readFromCRS and readToCRS indicate source and target coordinate reference system, respectively.
   */
  val dropInvalid = parameters.getOrElse("dropInvalid", "false").toBoolean
  val disableErrorInCRS = parameters.getOrElse("disableErrorInCRS", "false").toBoolean
  val readFromCRS = parameters.getOrElse("readFromCRS", "")
  val readToCRS = parameters.getOrElse("readToCRS", "")

}