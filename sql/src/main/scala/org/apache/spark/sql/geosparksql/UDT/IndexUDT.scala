/*
 * FILE: IndexUDT.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.geosparksql.UDT

import org.locationtech.jts.index.SpatialIndex
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, UserDefinedType}
import org.datasyslab.geosparksql.utils.IndexSerializer

class IndexUDT extends UserDefinedType[SpatialIndex] {
  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)

  override def serialize(obj: SpatialIndex): GenericArrayData = {
    return new GenericArrayData(IndexSerializer.serialize(obj))
  }

  override def deserialize(datum: Any): SpatialIndex = {
    datum match {
      case values: ArrayData => {
        return IndexSerializer.deserialize(values)
      }
    }
  }

  override def userClass: Class[SpatialIndex] = classOf[SpatialIndex]
}
