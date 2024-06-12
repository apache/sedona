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

import org.apache.sedona.common.raster.serde.Serde
import org.geotools.coverage.grid.GridCoverage2D

object RasterSerializer {

  /**
   * Given a raster returns array of bytes
   *
   * @param GridCoverage2D
   *   raster
   * @return
   *   Array of bites represents this geometry
   */
  def serialize(raster: GridCoverage2D): Array[Byte] = {
    Serde.serialize(raster);
  }

  /**
   * Given ArrayData returns Geometry
   *
   * @param value
   *   ArrayData
   * @return
   *   GridCoverage2D
   */
  def deserialize(value: Array[Byte]): GridCoverage2D = {
    Serde.deserialize(value);
  }
}
