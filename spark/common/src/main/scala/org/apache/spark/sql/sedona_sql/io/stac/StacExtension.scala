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
package org.apache.spark.sql.sedona_sql.io.stac

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Defines a STAC extension with its schema and property mappings
 *
 * @param name
 *   The name of the STAC extension (e.g., "eo", "proj")
 * @param schema
 *   The schema for the extension properties
 * @param propertyMappings
 *   Mapping from original property path to top-level property name
 */
case class StacExtension(name: String, schema: StructType)

object StacExtension {

  /**
   * Returns an array of STAC extension definitions, each containing:
   *   - Extension name
   *   - Extension schema
   *   - Mapping from original property paths to promoted top-level property names
   *
   * @return
   *   Array of StacExtension definitions
   */
  def getStacExtensionDefinitions(): Array[StacExtension] = {
    Array(
      // Grid extension - https://stac-extensions.github.io/grid/v1.1.0/schema.json
      StacExtension(
        name = "grid",
        // Schema for the grid extension, add all required fields here
        schema = StructType(Seq(StructField("grid:code", StringType, nullable = true))))

      // Add other extensions here...
    )
  }
}
