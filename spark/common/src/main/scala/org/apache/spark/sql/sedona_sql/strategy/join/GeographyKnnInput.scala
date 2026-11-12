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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.common.S2Geography.{Geography, WKBGeography, WKBWriter => GeographyWKBWriter}
import org.apache.sedona.common.geography.GeographyKnnIndex
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.locationtech.jts.io.ByteOrderValues

/**
 * One row of a geography KNN join in a form that every Spark serializer can ship: the geography
 * as WKB, and the row it came from. The S2 view of the geography is rebuilt once per JVM.
 */
private[join] final case class GeographyKnnInput(wkb: Array[Byte], srid: Int, row: UnsafeRow) {
  @transient lazy val geography: Geography = WKBGeography.fromWKB(wkb, srid)
}

private[join] object GeographyKnnInput {

  /**
   * Returns None for a NULL or empty geography. Such a row takes no part in a KNN join: as a
   * query it has no neighbours, and as an object it is nobody's neighbour.
   */
  def from(geography: Geography, row: UnsafeRow): Option[GeographyKnnInput] =
    if (GeographyKnnIndex.bounds(geography) == null) {
      None
    } else {
      val wkb = geography match {
        case wkbGeography: WKBGeography => wkbGeography.getWKBBytes
        case _ => new GeographyWKBWriter(2, ByteOrderValues.BIG_ENDIAN, false).write(geography)
      }
      Some(GeographyKnnInput(wkb, geography.getSRID, row))
    }
}
