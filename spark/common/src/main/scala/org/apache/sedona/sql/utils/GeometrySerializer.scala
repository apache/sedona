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
package org.apache.sedona.sql.utils

import org.apache.sedona.common.Constructors
import org.apache.sedona.common.geometrySerde.{GeometrySerializer => LayoutPolicy}
import org.apache.sedona.common.utils.GeomUtils
import org.locationtech.jts.geom.{Geometry, GeometryFactory}

/**
 * SQL geometry transport using public EWKB. RDD/index serializers use their separate compact
 * format.
 */
object GeometrySerializer {

  /**
   * Given a geometry returns array of bytes
   *
   * @param geometry
   *   JTS geometry
   * @return
   *   EWKB bytes preserving coordinate dimensions and SRID
   */
  def serialize(geometry: Geometry): Array[Byte] = {
    validateLayout(geometry)
    GeomUtils.getEWKB(geometry)
  }

  // Preserve the existing layout contract without using compact byte serialization.
  // General collections have independent member layouts; polygons and multipart values do not.
  private def validateLayout(geometry: Geometry): Unit = {
    if (geometry != null) {
      if (geometry.getGeometryType == "GeometryCollection") {
        (0 until geometry.getNumGeometries).foreach(i => validateLayout(geometry.getGeometryN(i)))
      } else {
        LayoutPolicy.getCoordinateType(geometry)
      }
    }
  }

  /**
   * Given EWKB bytes returns Geometry
   *
   * @param value
   *   EWKB bytes
   * @return
   *   JTS geometry
   */
  def deserialize(value: Array[Byte]): Geometry = {
    if (value == null) {
      return new GeometryFactory().createGeometryCollection()
    }
    Constructors.geomFromWKB(value)
  }
}
