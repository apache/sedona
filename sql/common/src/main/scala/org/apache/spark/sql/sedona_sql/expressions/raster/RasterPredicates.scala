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

package org.apache.spark.sql.sedona_sql.expressions.raster

import org.apache.sedona.common.raster.RasterPredicates
import org.apache.sedona.sql.utils.{GeometrySerializer, RasterSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT, RasterUDT}
import org.apache.spark.sql.sedona_sql.expressions.FoldableExpression
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType}
import org.geotools.coverage.grid.GridCoverage2D
import org.locationtech.jts.geom.Geometry

abstract class RS_Predicate extends Expression
  with FoldableExpression
  with ExpectsInputTypes
  with NullIntolerant {
  def inputExpressions: Seq[Expression]

  override def toString: String = s" **${this.getClass.getName}**  "

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[AbstractDataType] = Seq(RasterUDT, GeometryUDT)

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions

  override final def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions.head.eval(inputRow).asInstanceOf[Array[Byte]]
    if (leftArray == null) {
      null
    } else {
      val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
      if (rightArray == null) {
        null
      } else {
        val leftGeometry = RasterSerializer.deserialize(leftArray)
        val rightGeometry = GeometrySerializer.deserialize(rightArray)
        evalGeom(leftGeometry, rightGeometry)
      }
    }
  }

  def evalGeom(leftGeometry: GridCoverage2D, rightGeometry: Geometry): Boolean
}

case class RS_Intersects(inputExpressions: Seq[Expression])
  extends RS_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: GridCoverage2D, rightGeometry: Geometry): Boolean = {
    RasterPredicates.rsIntersects(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Contains(inputExpressions: Seq[Expression])
  extends RS_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: GridCoverage2D, rightGeometry: Geometry): Boolean = {
    RasterPredicates.rsContains(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class RS_Within(inputExpressions: Seq[Expression])
  extends RS_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: GridCoverage2D, rightGeometry: Geometry): Boolean = {
    RasterPredicates.rsWithin(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

