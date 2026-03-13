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

import org.apache.spark.sql.types._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JValue
import org.apache.sedona.common.S2Geography.{GeographySerializer, Geography}

class GeographyUDT extends UserDefinedType[Geography] {
  override def sqlType: DataType = BinaryType

  override def pyUDT: String = "sedona.spark.sql.types.GeographyType"

  override def userClass: Class[Geography] = classOf[Geography]

  override def serialize(obj: Geography): Array[Byte] =
    GeographySerializer.serialize(obj)

  override def deserialize(datum: Any): Geography = {
    datum match {
      case value: Array[Byte] => GeographySerializer.deserialize(value)
    }
  }

  override private[sql] def jsonValue: JValue = {
    super.jsonValue mapField {
      case ("class", _) => "class" -> this.getClass.getName.stripSuffix("$")
      case other: Any => other
    }
  }

  override def equals(other: Any): Boolean = other match {
    case _: UserDefinedType[_] => other.isInstanceOf[GeographyUDT]
    case _ => false
  }

  override def hashCode(): Int = userClass.hashCode()

  override def toString: String = "GeographyUDT"
}

case object GeographyUDT
    extends org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
    with scala.Serializable {
  def apply(): GeographyUDT = new GeographyUDT()
}
