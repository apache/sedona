/*
 * FILE: GeometryUDT.scala
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

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.datasyslab.geosparksql.utils.GeometrySerializer


private[sql] class GeometryUDT extends UserDefinedType[Geometry] {
  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)
  override def pyUDT: String = "geospark.sql.types.GeometryType"

  override def userClass: Class[Geometry] = classOf[Geometry]

  override def serialize(obj: Geometry): GenericArrayData =
    new GenericArrayData(GeometrySerializer.serialize(obj))

  override def deserialize(datum: Any): Geometry = {
    datum match {
      case values: ArrayData =>
        GeometrySerializer.deserialize(values)
    }
  }

  case object GeometryUDT extends GeometryUDT

}
