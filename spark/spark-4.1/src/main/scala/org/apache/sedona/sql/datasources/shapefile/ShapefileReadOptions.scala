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
package org.apache.sedona.sql.datasources.shapefile

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Options for reading Shapefiles.
 * @param geometryFieldName
 *   The name of the geometry field.
 * @param keyFieldName
 *   The name of the shape key field.
 * @param charset
 *   The charset of non-spatial attributes.
 */
case class ShapefileReadOptions(
    geometryFieldName: String,
    keyFieldName: Option[String],
    charset: Option[String])

object ShapefileReadOptions {
  def parse(options: CaseInsensitiveStringMap): ShapefileReadOptions = {
    val geometryFieldName = options.getOrDefault("geometry.name", "geometry")
    val keyFieldName =
      if (options.containsKey("key.name")) Some(options.get("key.name")) else None
    val charset = if (options.containsKey("charset")) Some(options.get("charset")) else None
    ShapefileReadOptions(geometryFieldName, keyFieldName, charset)
  }
}
