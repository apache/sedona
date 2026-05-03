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

import org.apache.sedona.common.geometryObjects.Box2D
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

/**
 * UDT for [[Box2D]]. Stored as a Spark struct of four non-nullable doubles (`xmin`, `ymin`,
 * `xmax`, `ymax`) so values round-trip natively to Parquet and align with GeoParquet 1.1 bbox
 * covering columns.
 */
class Box2DUDT extends UserDefinedType[Box2D] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false)))

  override def pyUDT: String = "sedona.spark.sql.types.Box2DType"

  override def userClass: Class[Box2D] = classOf[Box2D]

  override def serialize(obj: Box2D): InternalRow = {
    val row = new GenericInternalRow(4)
    row.setDouble(0, obj.getXMin)
    row.setDouble(1, obj.getYMin)
    row.setDouble(2, obj.getXMax)
    row.setDouble(3, obj.getYMax)
    row
  }

  override def deserialize(datum: Any): Box2D = datum match {
    case row: InternalRow =>
      new Box2D(row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3))
  }

  override private[sql] def jsonValue: JValue = {
    super.jsonValue mapField {
      case ("class", _) => "class" -> this.getClass.getName.stripSuffix("$")
      case other: Any => other
    }
  }

  override def equals(other: Any): Boolean = other match {
    case _: UserDefinedType[_] => other.isInstanceOf[Box2DUDT]
    case _ => false
  }

  override def hashCode(): Int = userClass.hashCode()

  override def toString: String = "Box2DUDT"
}

case object Box2DUDT
    extends org.apache.spark.sql.sedona_sql.UDT.Box2DUDT
    with scala.Serializable {
  def apply(): Box2DUDT = new Box2DUDT()
}
