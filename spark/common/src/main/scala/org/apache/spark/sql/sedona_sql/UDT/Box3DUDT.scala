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

import org.apache.sedona.common.geometryObjects.Box3D
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

/**
 * UDT for [[Box3D]]. Stored as a Spark struct of six non-nullable doubles in PostGIS `box3d`
 * order: `xmin`, `ymin`, `zmin`, `xmax`, `ymax`, `zmax`.
 */
class Box3DUDT extends UserDefinedType[Box3D] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("zmin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("zmax", DoubleType, nullable = false)))

  // No `pyUDT` override yet — the Python `Box3DType` class is intentionally out of scope for
  // Phase 1 (see #2973). It will be added together with the Python bindings follow-up, the
  // same way Box2D paired `Box2DUDT.pyUDT` with `python/sedona/spark/sql/types.py::Box2DType`.

  override def userClass: Class[Box3D] = classOf[Box3D]

  override def serialize(obj: Box3D): InternalRow = {
    val row = new GenericInternalRow(6)
    row.setDouble(0, obj.getXMin)
    row.setDouble(1, obj.getYMin)
    row.setDouble(2, obj.getZMin)
    row.setDouble(3, obj.getXMax)
    row.setDouble(4, obj.getYMax)
    row.setDouble(5, obj.getZMax)
    row
  }

  override def deserialize(datum: Any): Box3D = datum match {
    case row: InternalRow =>
      new Box3D(
        row.getDouble(0),
        row.getDouble(1),
        row.getDouble(2),
        row.getDouble(3),
        row.getDouble(4),
        row.getDouble(5))
  }

  override private[sql] def jsonValue: JValue = {
    super.jsonValue mapField {
      case ("class", _) => "class" -> this.getClass.getName.stripSuffix("$")
      case other: Any => other
    }
  }

  override def equals(other: Any): Boolean = other match {
    case _: UserDefinedType[_] => other.isInstanceOf[Box3DUDT]
    case _ => false
  }

  override def hashCode(): Int = userClass.hashCode()

  override def toString: String = "Box3DUDT"
}

case object Box3DUDT
    extends org.apache.spark.sql.sedona_sql.UDT.Box3DUDT
    with scala.Serializable {
  def apply(): Box3DUDT = new Box3DUDT()
}
