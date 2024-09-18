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
package org.apache.sedona.sql.datasources.geopackage.transform

import org.apache.sedona.sql.datasources.geopackage.model.{GeoPackageType, PartitionOptions, TileRowMetadata}
import org.apache.sedona.sql.datasources.geopackage.model.TableType.TILES
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.unsafe.types.UTF8String

object ValuesMapper {
  def mapValues(metadata: PartitionOptions, rs: java.sql.ResultSet): Seq[Any] = {
    metadata.columns.map(column => {
      (column.dataType, metadata.tableType) match {
        case (GeoPackageType.INTEGER | GeoPackageType.INT, _) => rs.getInt(column.name)
        case (GeoPackageType.TINY_INT, _) => rs.getInt(column.name)
        case (GeoPackageType.SMALLINT, _) => rs.getInt(column.name)
        case (GeoPackageType.MEDIUMINT, _) => rs.getInt(column.name)
        case (GeoPackageType.FLOAT, _) => rs.getFloat(column.name)
        case (GeoPackageType.DOUBLE, _) => rs.getDouble(column.name)
        case (GeoPackageType.REAL, _) => rs.getDouble(column.name)
        case (startsWith: String, _) if startsWith.startsWith(GeoPackageType.TEXT) =>
          UTF8String.fromString(rs.getString(column.name))
        case (startsWith: String, TILES)
            if startsWith.startsWith(GeoPackageType.BLOB) && column.name == "tile_data" =>
          metadata.tile match {
            case Some(value) =>
              value.tileRowMetadata
                .map(tileRowMetadata => {
                  RasterUDT.serialize(
                    Image.readImageFile(rs.getBytes(column.name), value, tileRowMetadata))
                })
                .orNull
            case None => null
          }
        case (startsWith: String, _) if startsWith.startsWith(GeoPackageType.BLOB) =>
          rs.getBytes(column.name)
        case (GeoPackageType.BOOLEAN, _) =>
          rs.getBoolean(column.name)
        case (GeoPackageType.DATE, _) =>
          DataTypesTransformations.getDays(rs.getString(column.name))
        case (GeoPackageType.DATETIME, _) =>
          DataTypesTransformations.epoch(rs.getString(column.name)) * 1000
        case (GeoPackageType.POINT, _) =>
          GeometryReader.extractWKB(rs.getBytes(column.name))
        case (GeoPackageType.LINESTRING, _) =>
          GeometryReader.extractWKB(rs.getBytes(column.name))
        case (GeoPackageType.POLYGON, _) =>
          GeometryReader.extractWKB(rs.getBytes(column.name))
        case (GeoPackageType.GEOMETRY, _) =>
          GeometryReader.extractWKB(rs.getBytes(column.name))
        case (GeoPackageType.MULTIPOINT, _) =>
          GeometryReader.extractWKB(rs.getBytes(column.name))
        case (GeoPackageType.MULTILINESTRING, _) =>
          GeometryReader.extractWKB(rs.getBytes(column.name))
        case (GeoPackageType.MULTIPOLYGON, _) =>
          GeometryReader.extractWKB(rs.getBytes(column.name))
        case (GeoPackageType.GEOMETRYCOLLECTION, _) =>
          GeometryReader.extractWKB(rs.getBytes(column.name))
        case _ =>
          UTF8String.fromString(rs.getString(column.name))
      }
    })
  }
}
