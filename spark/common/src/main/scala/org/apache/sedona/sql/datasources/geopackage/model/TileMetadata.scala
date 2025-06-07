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
package org.apache.sedona.sql.datasources.geopackage.model

import org.geotools.referencing.CRS
import org.geotools.api.referencing.crs.CoordinateReferenceSystem

import scala.collection.mutable

case class TileMetadata(
    tableName: String,
    minX: Double,
    minY: Double,
    maxX: Double,
    maxY: Double,
    srsID: Int,
    zoomLevelMetadata: mutable.HashMap[Int, TileMatrix],
    tileRowMetadata: Option[TileRowMetadata]) {

  def withTileRowMetadata(tileRowMetadata: TileRowMetadata): TileMetadata = {
    TileMetadata(
      tableName,
      minX,
      minY,
      maxX,
      maxY,
      srsID,
      zoomLevelMetadata,
      Some(tileRowMetadata))
  }
  def getSRID(): CoordinateReferenceSystem = {
    CRS.decode("EPSG:" + srsID)
  }

  def getEnvelope(tileRowMetadata: TileRowMetadata): Envelope = {
    val tileMatrix = zoomLevelMetadata(tileRowMetadata.zoomLevel)
    val numberOfRows = tileMatrix.matrixHeight
    val numberOfColumns = tileMatrix.matrixWidth
    val columnNumber = tileRowMetadata.tileColumn
    val rowNumber = tileRowMetadata.tileRow

    val dify = (this.maxY - this.minY)

    val minX = this.minX + (columnNumber * (this.maxX - this.minX) / numberOfColumns)
    val maxX = this.minX + ((columnNumber + 1) * (this.maxX - this.minX) / numberOfColumns)
    val minY = this.minY + ((numberOfRows - 1 - rowNumber) / numberOfRows.toDouble * dify)
    val maxY = this.minY + ((numberOfRows - rowNumber) / numberOfRows.toDouble * dify)

    Envelope(minX, minY, maxX, maxY)
  }
}
