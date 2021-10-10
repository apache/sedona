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
package org.apache.spark.sql.sedona_sql.expressions.geohash
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}


object PointGeoHashEncoder {
  private val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  private val bits = Seq(16, 8, 4, 2, 1)

  def calculateGeoHash(geom: Point, precision: Long): String = {
    val bbox = BBox(-180, 180, -90, 90)
    val precisionUpdated = math.min(precision, 20)
    if (precision <= 0) ""
    else geoHashAggregate(
      geom, precisionUpdated, 0, "", true, bbox, 0, 0
    )

  }

  private def geoHashAggregate(point: Point, precision: Long, currentPrecision: Long,
                               geoHash: String, isEven: Boolean, bbox: BBox, bit: Int, ch: Int): String = {
    if (currentPrecision >= precision) geoHash
    else {
      val geoHashUpdate = if (isEven){
        val mid = (bbox.startLon + bbox.endLon) / 2.0
        if (point.getX >= mid) GeoHashUpdate(bbox.copy(startLon = mid), ch | bits(bit))
        else GeoHashUpdate(bbox.copy(endLon = mid), ch)
      }else {
        val mid = (bbox.startLat + bbox.endLat) / 2.0
        if (point.getY >= mid) GeoHashUpdate(bbox.copy(startLat = mid), ch | bits(bit))
        else GeoHashUpdate(bbox.copy(endLat = mid), ch)
      }
      if (bit < 4) {

        geoHashAggregate(
          point,
          precision,
          currentPrecision,
          geoHash,
          !isEven,
          geoHashUpdate.bbox,
          bit + 1,
          geoHashUpdate.ch
        )
      }
      else {
        val geoHashUpdated = geoHash + base32(geoHashUpdate.ch)
        geoHashAggregate(
          point,
          precision,
          currentPrecision + 1,
          geoHashUpdated,
          !isEven,
          geoHashUpdate.bbox,
          0,
          0
        )
      }
    }
  }
}

private[geohash] sealed case class BBox(startLon: Double, endLon: Double, startLat: Double, endLat: Double){
  private val geometryFactory = new GeometryFactory()
  def getCentroid(): Point = {
    val lon = startLon + ((startLon + endLon)/2)
    val lat = startLat + ((startLat + endLat)/2)
    geometryFactory.createPoint(new Coordinate(lon, lat))
  }

  def toPolygon(): Polygon = {
    geometryFactory.createPolygon(
      Array(
        new Coordinate(startLon, startLat),
        new Coordinate(startLon, endLat),
        new Coordinate(endLon, endLat),
        new Coordinate(endLon, startLat),
        new Coordinate(startLon, startLat)
      )
    )
  }
}

private[geohash] sealed case class GeoHashUpdate(bbox: BBox, ch: Int)