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
import org.locationtech.jts.geom.{Geometry, Point}

import scala.annotation.tailrec

object PointGeoHashCalculator {
  private val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  private val bits = Seq(16, 8, 4, 2, 1)

  def calculateGeoHash(geom: Point, precision: Long): Option[String] = {
    val bbox = BBox(-180, 180, -90, 90)
    geoHashAggregate(
      geom, precision, 0, "", true, bbox, 0, 0
    )

  }

  @tailrec
  private def geoHashAggregate(point: Point, precision: Long, currentPrecision: Long,
                               geoHash: String, isEven: Boolean, bbox: BBox, bit: Int, ch: Int): Option[String] = {
    if (currentPrecision >= precision) Some(geoHash)
    else if (precision > 20) None
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

private[geohash] sealed case class BBox(startLon: BigDecimal, endLon: BigDecimal, startLat: BigDecimal, endLat: BigDecimal){
  override def toString: String = s"lon1: $startLon lon2: $endLon lat1: $startLat lat2: $endLat"
}

private[geohash] sealed case class GeoHashUpdate(bbox: BBox, ch: Int)