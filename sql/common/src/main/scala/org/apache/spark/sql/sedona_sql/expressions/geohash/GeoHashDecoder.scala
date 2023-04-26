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

import org.apache.sedona.common.utils.BBox
import org.locationtech.jts.geom.{Geometry, GeometryFactory}

import scala.collection.mutable

class InvalidGeoHashException(message: String) extends Exception(message)

object GeoHashDecoder {
  private val bits = Seq(16, 8, 4, 2, 1)
  private val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  val geometryFactory = new GeometryFactory()

  def decode(geohash: String, precision: Option[Int] = None): Geometry = {
    decodeGeoHashBBox(geohash, precision).getBbox().toPolygon()
  }

  private def decodeGeoHashBBox(geohash: String, precision: Option[Int] = None): LatLon = {
    val latLon = LatLon(mutable.Seq(-180, 180), mutable.Seq(-90, 90))
    val geoHashLowered = geohash.toLowerCase()
    val geoHashLength = geohash.length
    val targetPrecision = precision match {
      case Some(value) => if (value < 0) throw new InvalidGeoHashException("Precision can not be negative")
                          else math.min(geoHashLength, value)
      case None => geoHashLength
    }
    var isEven = true

    for (i <- Range(0, targetPrecision)){
      val c = geoHashLowered(i)
      val cd = base32.indexOf(c).toByte
      if (cd == -1){
        throw new InvalidGeoHashException(s"Invalid character '$c' found at index $i")
      }

      for (j <- Range(0, 5)){
        val mask = bits(j).toByte
        val index = if ((mask & cd) == 0) 1 else 0
        if (isEven){
          latLon.lons(index) = (latLon.lons.head + latLon.lons(1)) / 2
        }
        else {
          latLon.lats(index) = (latLon.lats.head + latLon.lats(1)) / 2
        }
        isEven = !isEven
      }
    }
    latLon
  }
}

private case class LatLon(var lons: mutable.Seq[Double], var lats: mutable.Seq[Double]){
  def getBbox(): BBox = new BBox(
    lons.head,
    lons(1),
    lats.head,
    lats(1)
  )
}
