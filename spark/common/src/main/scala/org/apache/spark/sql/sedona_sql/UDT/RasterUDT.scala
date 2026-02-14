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

import org.apache.sedona.common.raster.serde.Serde
import org.apache.spark.sql.types.{BinaryType, DataType, UserDefinedType}
import org.geotools.coverage.grid.GridCoverage2D
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JValue

class RasterUDT extends UserDefinedType[GridCoverage2D] {
  override def sqlType: DataType = BinaryType

  override def pyUDT: String = "sedona.spark.sql.types.RasterType"

  // A reasonable size for small in-db rasters. This is used by the optimizer to decide whether to
  // broadcast the dataframe or not.
  override def defaultSize: Int = 512 * 1024

  override def serialize(raster: GridCoverage2D): Array[Byte] = Serde.serialize(raster)

  override def deserialize(datum: Any): GridCoverage2D = {
    datum match {
      case bytes: Array[Byte] => Serde.deserialize(bytes)
    }
  }

  override def userClass: Class[GridCoverage2D] = classOf[GridCoverage2D]

  override private[sql] def jsonValue: JValue = {
    super.jsonValue mapField {
      case ("class", _) => "class" -> this.getClass.getName.stripSuffix("$")
      case other: Any => other
    }
  }

  override def equals(other: Any): Boolean = other match {
    case _: UserDefinedType[_] => other.isInstanceOf[RasterUDT]
    case _ => false
  }

  override def hashCode(): Int = userClass.hashCode()

  override def toString: String = "RasterUDT"
}

case object RasterUDT extends RasterUDT with Serializable {
  def apply(): RasterUDT = new RasterUDT()
}
