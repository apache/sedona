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

import org.apache.sedona.sql.datasources.geopackage.model.TableType.TableType
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.types._

case class GeoPackageField(name: String, dataType: String, isNullable: Boolean) {
  override def toString: String = {
    s"GeoPackageField(name=$name, dataType=$dataType, isNullable=$isNullable)"
  }

  def toStructField(tableType: TableType): StructField = {
    dataType match {
      case startsWith: String if startsWith.startsWith(GeoPackageType.TEXT) =>
        StructField(name, StringType)
      case startsWith: String if startsWith.startsWith(GeoPackageType.BLOB) => {
        if (tableType == TableType.TILES) {
          return StructField(name, RasterUDT())
        }

        StructField(name, BinaryType)
      }
      case GeoPackageType.INTEGER | GeoPackageType.INT | GeoPackageType.SMALLINT |
          GeoPackageType.TINY_INT | GeoPackageType.MEDIUMINT =>
        StructField(name, IntegerType)
      case GeoPackageType.POINT => StructField(name, GeometryUDT())
      case GeoPackageType.LINESTRING => StructField(name, GeometryUDT())
      case GeoPackageType.POLYGON => StructField(name, GeometryUDT())
      case GeoPackageType.GEOMETRY => StructField(name, GeometryUDT())
      case GeoPackageType.MULTIPOINT => StructField(name, GeometryUDT())
      case GeoPackageType.MULTILINESTRING => StructField(name, GeometryUDT())
      case GeoPackageType.MULTIPOLYGON => StructField(name, GeometryUDT())
      case GeoPackageType.GEOMETRYCOLLECTION => StructField(name, GeometryUDT())
      case GeoPackageType.REAL => StructField(name, DoubleType)
      case GeoPackageType.BOOLEAN => StructField(name, BooleanType)
      case GeoPackageType.DATE => StructField(name, DateType)
      case GeoPackageType.DATETIME => StructField(name, TimestampType)
      case GeoPackageType.FLOAT => StructField(name, FloatType)
      case GeoPackageType.DOUBLE => StructField(name, DoubleType)
      case _ => StructField(name, StringType)
    }
  }
}
