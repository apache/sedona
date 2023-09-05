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
package org.apache.spark.sql.sedona_sql.UDT


import org.apache.sedona.common.raster.Serde
import org.apache.spark.sql.types.{BinaryType, DataType, UserDefinedType}
import org.geotools.coverage.grid.GridCoverage2D

class RasterUDT extends UserDefinedType[GridCoverage2D] {
  override def sqlType: DataType = BinaryType

  override def serialize(raster: GridCoverage2D): Array[Byte] = Serde.serialize(raster)

  override def deserialize(datum: Any): GridCoverage2D = {
    datum match {
      case bytes: Array[Byte] => Serde.deserialize(bytes)
    }
  }

  override def userClass: Class[GridCoverage2D] = classOf[GridCoverage2D]
}

case object RasterUDT extends RasterUDT with Serializable