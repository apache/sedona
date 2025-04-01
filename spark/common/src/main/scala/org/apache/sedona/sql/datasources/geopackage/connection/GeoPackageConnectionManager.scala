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
package org.apache.sedona.sql.datasources.geopackage.connection

import org.apache.sedona.sql.datasources.geopackage.model.{GeoPackageField, TableType, TileMatrix, TileMetadata}
import org.apache.sedona.sql.datasources.geopackage.model.TableType.TableType

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GeoPackageConnectionManager {

  def createStatement(file: String): Statement = {
    val conn = DriverManager.getConnection("jdbc:sqlite:" + file)
    conn.createStatement()
  }

  def closeStatement(st: Statement): Unit = {
    st.close()
  }

  def getTableCursor(path: String, tableName: String): ResultSet = {
    val conn: Connection =
      DriverManager.getConnection("jdbc:sqlite:" + path)
    val stmt: Statement = conn.createStatement()
    stmt.executeQuery(s"SELECT * FROM ${tableName}")
  }

  def getSchema(file: String, tableName: String): Seq[GeoPackageField] = {
    val statement = createStatement(file)

    try {
      val rs = statement.executeQuery(s"PRAGMA table_info($tableName)")
      val fields = ArrayBuffer.empty[GeoPackageField]

      while (rs.next) {
        val columnName = rs.getString("name")
        val columnType = rs.getString("type")

        fields += GeoPackageField(columnName, columnType, true)
      }

      fields.toSeq
    } finally {
      closeStatement(statement)
    }
  }

  def findFeatureMetadata(file: String, tableName: String): TableType = {
    val statement = createStatement(file)

    val rs =
      statement.executeQuery(s"select * from gpkg_contents where table_name = '$tableName'")

    try {
      rs.getString("data_type") match {
        case "features" => TableType.FEATURES
        case "tiles" => TableType.TILES
        case _ => TableType.UNKNOWN
      }
    } finally {
      rs.close()
      closeStatement(statement)
    }
  }

  def getZoomLevelData(file: String, tableName: String): mutable.HashMap[Int, TileMatrix] = {
    val stmt = createStatement(file)
    val rs =
      stmt.executeQuery(f"select * from gpkg_tile_matrix where table_name = '${tableName}'")
    val result: mutable.HashMap[Int, TileMatrix] = mutable.HashMap()

    try {
      while (rs.next()) {
        val zoom_level = rs.getInt("zoom_level")
        val matrix_width = rs.getInt("matrix_width")
        val matrix_height = rs.getInt("matrix_height")
        val tile_width = rs.getInt("tile_width")
        val tile_height = rs.getInt("tile_height")
        val pixel_x_size = rs.getDouble("pixel_x_size")
        val pixel_y_size = rs.getDouble("pixel_y_size")

        result(zoom_level) = TileMatrix(
          zoom_level,
          matrix_width,
          matrix_height,
          tile_width,
          tile_height,
          pixel_x_size,
          pixel_y_size)
      }
    } finally {
      rs.close()
      closeStatement(stmt)
    }

    result
  }

  def findTilesMetadata(file: String, tableName: String): TileMetadata = {
    val statement = createStatement(file)

    val rs = statement.executeQuery(
      s"select * from gpkg_tile_matrix_set where table_name = '$tableName'")

    try {
      val minX = rs.getDouble("min_x")
      val minY = rs.getDouble("min_y")
      val maxX = rs.getDouble("max_x")
      val maxY = rs.getDouble("max_y")
      val srsID = rs.getInt("srs_id")

      val getZoomLevelData = GeoPackageConnectionManager.getZoomLevelData(file, tableName)

      TileMetadata(
        tableName = tableName,
        minX = minX,
        minY = minY,
        maxX = maxX,
        maxY = maxY,
        srsID = srsID,
        zoomLevelMetadata = getZoomLevelData,
        tileRowMetadata = null)
    } finally {
      rs.close()
      closeStatement(statement)
    }
  }
}
