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
package org.apache.sedona.sql.datasources.geopackage

import org.apache.hadoop.fs.Path
import org.apache.sedona.sql.datasources.geopackage.model.GeoPackageOptions
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.Try

class GeoPackageDataSource extends FileDataSourceV2 with DataSourceRegister {

  override def fallbackFileFormat: Class[_ <: FileFormat] = {
    null
  }

  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    GeoPackageTable(
      "",
      sparkSession,
      options,
      getPaths(options),
      None,
      fallbackFileFormat,
      getLoadOptions(options))
  }

  private def getLoadOptions(options: CaseInsensitiveStringMap): GeoPackageOptions = {
    val path = options.get("path")
    if (path.isEmpty) {
      throw new IllegalArgumentException("GeoPackage path is not specified")
    }

    val showMetadata = options.getBoolean("showMetadata", false)
    val maybeTableName = options.get("tableName")

    if (!showMetadata && maybeTableName == null) {
      throw new IllegalArgumentException("Table name is not specified")
    }

    val tableName = if (showMetadata) {
      "gpkg_contents"
    } else {
      maybeTableName
    }

    GeoPackageOptions(tableName = tableName, showMetadata = showMetadata)
  }

  override def shortName(): String = "geopackage"
}
