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
  def ST_GeomFromGeoHash(geohash: Column, precision: Column): Column =
    wrapExpression[ST_GeomFromGeoHash](geohash, precision)
  def ST_GeomFromGeoHash(geohash: String, precision: Int): Column =
    wrapExpression[ST_GeomFromGeoHash](geohash, precision)

  def ST_GeomFromGeoHash(geohash: Column): Column =
    wrapExpression[ST_GeomFromGeoHash](geohash, null)

  def ST_GeomFromGeoHash(geohash: String): Column =
    wrapExpression[ST_GeomFromGeoHash](geohash, null)

  def ST_PointFromGeoHash(geohash: Column, precision: Column): Column =
    wrapExpression[ST_PointFromGeoHash](geohash, precision)
  def ST_PointFromGeoHash(geohash: String, precision: Int): Column =
    wrapExpression[ST_PointFromGeoHash](geohash, precision)

  def ST_PointFromGeoHash(geohash: Column): Column =
    wrapExpression[ST_PointFromGeoHash](geohash, null)

  def ST_PointFromGeoHash(geohash: String): Column =
    wrapExpression[ST_PointFromGeoHash](geohash, null)

  def ST_GeomFromGeoJSON(geojsonString: Column): Column =
    wrapExpression[ST_GeomFromGeoJSON](geojsonString)
  def ST_GeomFromGeoJSON(geojsonString: String): Column =
    wrapExpression[ST_GeomFromGeoJSON](geojsonString)

  def ST_GeomFromGML(gmlString: Column): Column = wrapExpression[ST_GeomFromGML](gmlString)
  def ST_GeomFromGML(gmlString: String): Column = wrapExpression[ST_GeomFromGML](gmlString)

  def ST_GeomFromKML(kmlString: Column): Column = wrapExpression[ST_GeomFromKML](kmlString)
  def ST_GeomFromKML(kmlString: String): Column = wrapExpression[ST_GeomFromKML](kmlString)

  def ST_GeomFromText(wkt: Column): Column = wrapExpression[ST_GeomFromText](wkt, 0)
  def ST_GeomFromText(wkt: String): Column = wrapExpression[ST_GeomFromText](wkt, 0)

  def ST_GeomFromText(wkt: Column, srid: Column): Column =
    wrapExpression[ST_GeomFromText](wkt, srid)

  def ST_GeomFromText(wkt: String, srid: Int): Column = wrapExpression[ST_GeomFromText](wkt, srid)

  def ST_GeometryFromText(wkt: Column): Column = wrapExpression[ST_GeometryFromText](wkt, 0)
  def ST_GeometryFromText(wkt: String): Column = wrapExpression[ST_GeometryFromText](wkt, 0)

  def ST_GeometryFromText(wkt: Column, srid: Column): Column =
    wrapExpression[ST_GeometryFromText](wkt, srid)

  def ST_GeometryFromText(wkt: String, srid: Int): Column =
    wrapExpression[ST_GeometryFromText](wkt, srid)

  def ST_GeomFromWKB(wkb: Column): Column = wrapExpression[ST_GeomFromWKB](wkb)
  def ST_GeomFromWKB(wkb: String): Column = wrapExpression[ST_GeomFromWKB](wkb)

  def ST_GeomFromEWKB(wkb: Column): Column = wrapExpression[ST_GeomFromEWKB](wkb)
  def ST_GeomFromEWKB(wkb: String): Column = wrapExpression[ST_GeomFromEWKB](wkb)

  def ST_GeomFromWKT(wkt: Column): Column = wrapExpression[ST_GeomFromWKT](wkt, 0)
  def ST_GeomFromWKT(wkt: String): Column = wrapExpression[ST_GeomFromWKT](wkt, 0)

  def ST_GeomFromWKT(wkt: Column, srid: Column): Column =
    wrapExpression[ST_GeomFromWKT](wkt, srid)

  def ST_GeomFromWKT(wkt: String, srid: Int): Column = wrapExpression[ST_GeomFromWKT](wkt, srid)

  def ST_GeomFromEWKT(wkt: Column): Column = wrapExpression[ST_GeomFromEWKT](wkt)
  def ST_GeomFromEWKT(wkt: String): Column = wrapExpression[ST_GeomFromEWKT](wkt)

  def ST_LineFromText(wkt: Column): Column = wrapExpression[ST_LineFromText](wkt)
  def ST_LineFromText(wkt: String): Column = wrapExpression[ST_LineFromText](wkt)

  def ST_LineStringFromText(coords: Column, delimiter: Column): Column =
    wrapExpression[ST_LineStringFromText](coords, delimiter)
  def ST_LineStringFromText(coords: String, delimiter: String): Column =
    wrapExpression[ST_LineStringFromText](coords, delimiter)

  def ST_Point(x: Column, y: Column): Column = wrapExpression[ST_Point](x, y)
  def ST_Point(x: String, y: String): Column = wrapExpression[ST_Point](x, y)
  def ST_Point(x: Double, y: Double): Column = wrapExpression[ST_Point](x, y)

  def ST_PointZ(x: Column, y: Column, z: Column): Column = wrapExpression[ST_PointZ](x, y, z, 0)
  def ST_PointZ(x: String, y: String, z: String): Column = wrapExpression[ST_PointZ](x, y, z, 0)
  def ST_PointZ(x: Double, y: Double, z: Double): Column = wrapExpression[ST_PointZ](x, y, z, 0)

  def ST_PointZ(x: Column, y: Column, z: Column, srid: Column): Column =
    wrapExpression[ST_PointZ](x, y, z, srid)

  def ST_PointZ(x: String, y: String, z: String, srid: Column): Column =
    wrapExpression[ST_PointZ](x, y, z, srid)

  def ST_PointZ(x: Double, y: Double, z: Double, srid: Int): Column =
    wrapExpression[ST_PointZ](x, y, z, srid)

  def ST_PointM(x: Column, y: Column, m: Column): Column = wrapExpression[ST_PointM](x, y, m, 0)
  def ST_PointM(x: String, y: String, m: String): Column = wrapExpression[ST_PointM](x, y, m, 0)
  def ST_PointM(x: Double, y: Double, m: Double): Column = wrapExpression[ST_PointM](x, y, m, 0)

  def ST_PointM(x: Column, y: Column, m: Column, srid: Column): Column =
    wrapExpression[ST_PointM](x, y, m, srid)

  def ST_PointM(x: String, y: String, m: String, srid: Column): Column =
    wrapExpression[ST_PointM](x, y, m, srid)

  def ST_PointM(x: Double, y: Double, m: Double, srid: Int): Column =
    wrapExpression[ST_PointM](x, y, m, srid)

  def ST_PointZM(x: Column, y: Column, z: Column, m: Column): Column =
    wrapExpression[ST_PointZM](x, y, z, m, 0)
  def ST_PointZM(x: String, y: String, z: String, m: String): Column =
    wrapExpression[ST_PointZM](x, y, z, m, 0)
  def ST_PointZM(x: Double, y: Double, z: Double, m: Double): Column =
    wrapExpression[ST_PointZM](x, y, z, m, 0)

  def ST_PointZM(x: Column, y: Column, z: Column, m: Column, srid: Column): Column =
    wrapExpression[ST_PointZM](x, y, z, m, srid)

  def ST_PointZM(x: String, y: String, z: String, m: String, srid: Column): Column =
    wrapExpression[ST_PointZM](x, y, z, m, srid)

  def ST_PointZM(x: Double, y: Double, z: Double, m: Double, srid: Int): Column =
    wrapExpression[ST_PointZM](x, y, z, m, srid)

  def ST_PointFromWKB(wkb: Column): Column = wrapExpression[ST_PointFromWKB](wkb)
  def ST_PointFromWKB(wkb: String): Column = wrapExpression[ST_PointFromWKB](wkb)

  def ST_PointFromWKB(wkb: Column, srid: Column): Column =
    wrapExpression[ST_PointFromWKB](wkb, srid)
  def ST_PointFromWKB(wkb: String, srid: Int): Column = wrapExpression[ST_PointFromWKB](wkb, srid)

  def ST_LineFromWKB(wkb: Column): Column = wrapExpression[ST_LineFromWKB](wkb)
  def ST_LineFromWKB(wkb: String): Column = wrapExpression[ST_LineFromWKB](wkb)

  def ST_LineFromWKB(wkb: Column, srid: Column): Column =
    wrapExpression[ST_LineFromWKB](wkb, srid)
  def ST_LineFromWKB(wkb: String, srid: Int): Column = wrapExpression[ST_LineFromWKB](wkb, srid)

  def ST_LinestringFromWKB(wkb: Column): Column = wrapExpression[ST_LinestringFromWKB](wkb)
  def ST_LinestringFromWKB(wkb: String): Column = wrapExpression[ST_LinestringFromWKB](wkb)
  def ST_LinestringFromWKB(wkb: Column, srid: Column): Column =
    wrapExpression[ST_LinestringFromWKB](wkb, srid)
  def ST_LinestringFromWKB(wkb: String, srid: Int): Column =
    wrapExpression[ST_LinestringFromWKB](wkb, srid)

  def ST_MakePointM(x: Column, y: Column, m: Column): Column =
    wrapExpression[ST_MakePointM](x, y, m)
  def ST_MakePointM(x: String, y: String, m: String): Column =
    wrapExpression[ST_MakePointM](x, y, m)
  def ST_MakePointM(x: Double, y: Double, m: Double): Column =
    wrapExpression[ST_MakePointM](x, y, m)

  def ST_MakePoint(x: Column, y: Column): Column = wrapExpression[ST_MakePoint](x, y, null, null)
  def ST_MakePoint(x: String, y: String): Column = wrapExpression[ST_MakePoint](x, y, null, null)
  def ST_MakePoint(x: Double, y: Double): Column = wrapExpression[ST_MakePoint](x, y, null, null)

  def ST_MakePoint(x: Column, y: Column, z: Column): Column =
    wrapExpression[ST_MakePoint](x, y, z, null)
  def ST_MakePoint(x: String, y: String, z: String): Column =
    wrapExpression[ST_MakePoint](x, y, z, null)
  def ST_MakePoint(x: Double, y: Double, z: Double): Column =
    wrapExpression[ST_MakePoint](x, y, z, null)

  def ST_MakePoint(x: Column, y: Column, z: Column, m: Column): Column =
    wrapExpression[ST_MakePoint](x, y, z, m)
  def ST_MakePoint(x: String, y: String, z: String, m: String): Column =
    wrapExpression[ST_MakePoint](x, y, z, m)
  def ST_MakePoint(x: Double, y: Double, z: Double, m: Double): Column =
    wrapExpression[ST_MakePoint](x, y, z, m)

  def ST_PointFromText(coords: Column, delimiter: Column): Column =
    wrapExpression[ST_PointFromText](coords, delimiter)
  def ST_PointFromText(coords: String, delimiter: String): Column =
    wrapExpression[ST_PointFromText](coords, delimiter)

  def ST_PolygonFromEnvelope(minX: Column, minY: Column, maxX: Column, maxY: Column): Column =
    wrapExpression[ST_PolygonFromEnvelope](minX, minY, maxX, maxY)
  def ST_PolygonFromEnvelope(minX: String, minY: String, maxX: String, maxY: String): Column =
    wrapExpression[ST_PolygonFromEnvelope](minX, minY, maxX, maxY)
  def ST_PolygonFromEnvelope(minX: Double, minY: Double, maxX: Double, maxY: Double): Column =
    wrapExpression[ST_PolygonFromEnvelope](minX, minY, maxX, maxY)

  def ST_PolygonFromText(coords: Column, delimiter: Column): Column =
    wrapExpression[ST_PolygonFromText](coords, delimiter)
  def ST_PolygonFromText(coords: String, delimiter: String): Column =
    wrapExpression[ST_PolygonFromText](coords, delimiter)

  def ST_MPolyFromText(wkt: Column): Column = wrapExpression[ST_MPolyFromText](wkt, 0)
  def ST_MPolyFromText(wkt: String): Column = wrapExpression[ST_MPolyFromText](wkt, 0)

  def ST_MPolyFromText(wkt: Column, srid: Column): Column =
    wrapExpression[ST_MPolyFromText](wkt, srid)
  def ST_MPolyFromText(wkt: String, srid: Int): Column =
    wrapExpression[ST_GeomFromText](wkt, srid)

  def ST_MLineFromText(wkt: Column): Column = wrapExpression[ST_MLineFromText](wkt, 0)
  def ST_MLineFromText(wkt: String): Column = wrapExpression[ST_MLineFromText](wkt, 0)

  def ST_MLineFromText(wkt: Column, srid: Column): Column =
    wrapExpression[ST_MLineFromText](wkt, srid)

  def ST_MLineFromText(wkt: String, srid: Int): Column =
    wrapExpression[ST_MLineFromText](wkt, srid)

  def ST_GeomCollFromText(wkt: Column): Column = wrapExpression[ST_GeomCollFromText](wkt, 0)
  def ST_GeomCollFromText(wkt: String): Column = wrapExpression[ST_GeomCollFromText](wkt, 0)
  def ST_GeomCollFromText(wkt: Column, srid: Column): Column =
    wrapExpression[ST_GeomCollFromText](wkt, srid)
  def ST_GeomCollFromText(wkt: String, srid: Int): Column =
    wrapExpression[ST_GeomCollFromText](wkt, srid)

  def ST_MPointFromText(wkt: Column): Column = wrapExpression[ST_MPointFromText](wkt, 0)
  def ST_MPointFromText(wkt: String): Column = wrapExpression[ST_MPointFromText](wkt, 0)
  def ST_MPointFromText(wkt: Column, srid: Column): Column =
    wrapExpression[ST_MPointFromText](wkt, srid)
  def ST_MPointFromText(wkt: String, srid: Int): Column =
    wrapExpression[ST_MPointFromText](wkt, srid)
}
