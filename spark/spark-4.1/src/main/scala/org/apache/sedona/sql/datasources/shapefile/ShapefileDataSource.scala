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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Locale
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * A Spark SQL data source for reading ESRI Shapefiles. This data source supports reading the
 * following components of shapefiles:
 *
 * <ul> <li>.shp: the main file <li>.dbf: (optional) the attribute file <li>.shx: (optional) the
 * index file <li>.cpg: (optional) the code page file <li>.prj: (optional) the projection file
 * </ul>
 *
 * <p>The load path can be a directory containing the shapefiles, or a path to the .shp file. If
 * the path refers to a .shp file, the data source will also read other components such as .dbf
 * and .shx files in the same directory.
 */
class ShapefileDataSource extends FileDataSourceV2 with DataSourceRegister {

  override def shortName(): String = "shapefile"

  override def fallbackFileFormat: Class[_ <: FileFormat] = null

  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getTransformedPath(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    ShapefileTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }

  override protected def getTable(
      options: CaseInsensitiveStringMap,
      schema: StructType): Table = {
    val paths = getTransformedPath(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    ShapefileTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      Some(schema),
      fallbackFileFormat)
  }

  private def getTransformedPath(options: CaseInsensitiveStringMap): Seq[String] = {
    val paths = getPaths(options)
    transformPaths(paths, options)
  }

  private def transformPaths(
      paths: Seq[String],
      options: CaseInsensitiveStringMap): Seq[String] = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    paths.map { pathString =>
      if (pathString.toLowerCase(Locale.ROOT).endsWith(".shp")) {
        // If the path refers to a file, we need to change it to a glob path to support reading
        // .dbf and .shx files as well. For example, if the path is /path/to/file.shp, we need to
        // change it to /path/to/file.???
        val path = new Path(pathString)
        val fs = path.getFileSystem(hadoopConf)
        val isDirectory = Try(fs.getFileStatus(path).isDirectory).getOrElse(false)
        if (isDirectory) {
          pathString
        } else {
          pathString.substring(0, pathString.length - 3) + "???"
        }
      } else {
        pathString
      }
    }
  }
}
