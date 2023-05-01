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
package org.apache.spark.sql.sedona_sql.expressions.collect


import org.apache.sedona.common.Functions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.expressions.SerdeAware
import org.apache.spark.sql.types.{ArrayType, _}
import org.locationtech.jts.geom.Geometry

case class ST_Collect(inputExpressions: Seq[Expression])
    extends Expression
    with SerdeAware
    with CodegenFallback {
  assert(inputExpressions.length >= 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    evalWithoutSerialization(input).asInstanceOf[Geometry].toGenericArrayData
  }

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val firstElement = inputExpressions.head

    firstElement.dataType match {
      case ArrayType(elementType, _) =>
        elementType match {
          case _: GeometryUDT =>
            val data = firstElement.eval(input).asInstanceOf[ArrayData]
            val numElements = data.numElements()
            val geomElements = (0 until numElements)
              .map(element => data.getBinary(element))
              .filter(_ != null)
              .map(_.toGeometry)

            Functions.createMultiGeometry(geomElements.toArray)
          case _ => Functions.createMultiGeometry(Array())
        }
      case _ =>
        val geomElements =
          inputExpressions.map(_.toGeometry(input)).filter(_ != null)
        Functions.createMultiGeometry(geomElements.toArray)
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

}
