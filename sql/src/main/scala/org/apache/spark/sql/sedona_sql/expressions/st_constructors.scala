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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.Column

object st_constructors extends DataFrameAPI {
  def ST_GeomFromGeoHash(geohash: ColumnOrName, precision: ColumnOrName): Column = wrapExpression[ST_GeomFromGeoHash](geohash, precision)
  def ST_GeomFromGeoHash(geohash: ColumnOrName, precision: Int): Column = wrapExpression[ST_GeomFromGeoHash](geohash, precision)

  def ST_GeomFromGeoJSON(geojsonString: ColumnOrName): Column = wrapExpression[ST_GeomFromGeoJSON](geojsonString)

  def ST_GeomFromGML(gmlString: ColumnOrName): Column = wrapExpression[ST_GeomFromGML](gmlString)

  def ST_GeomFromKML(kmlString: ColumnOrName): Column = wrapExpression[ST_GeomFromKML](kmlString)

  def ST_GeomFromText(wkt: ColumnOrName): Column = wrapExpression[ST_GeomFromText](wkt)

  def ST_GeomFromWKB(wkb: ColumnOrName): Column = wrapExpression[ST_GeomFromWKB](wkb)

  def ST_GeomFromWKT(wkt: ColumnOrName): Column = wrapExpression[ST_GeomFromWKT](wkt)

  def ST_LineFromText(wkt: ColumnOrName): Column = wrapExpression[ST_LineFromText](wkt)

  def ST_LineStringFromText(coords: ColumnOrName, delimiter: ColumnOrName): Column = wrapExpression[ST_LineStringFromText](coords, delimiter)

  def ST_Point(x: ColumnOrNameOrNumber, y: ColumnOrNameOrNumber): Column = wrapExpression[ST_Point](x, y)
  def ST_Point(x: ColumnOrNameOrNumber, y: ColumnOrNameOrNumber, z: ColumnOrNameOrNumber): Column = wrapExpression[ST_Point](x, y, z)
 
  def ST_PointFromText(coords: ColumnOrName, delimiter: ColumnOrName): Column = wrapExpression[ST_PointFromText](coords, delimiter)

  def ST_PolygonFromEnvelope(minX: ColumnOrNameOrNumber, minY: ColumnOrNameOrNumber, maxX: ColumnOrNameOrNumber, maxY: ColumnOrNameOrNumber): Column = wrapExpression[ST_PolygonFromEnvelope](minX, minY, maxX, maxY)

  def ST_PolygonFromText(coords: ColumnOrName, delimiter: ColumnOrName): Column = wrapExpression[ST_PolygonFromText](coords, delimiter)
}
