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
package org.apache.spark.sql.sedona_sql.expressions

import com.google.common.geometry.S2BooleanOperation
import org.apache.sedona.common.S2Geography.{Predicates, S2Geography, S2GeographySerializer, ShapeIndexGeography}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType}

abstract class ST_S2Predicate
    extends Expression
    with FoldableExpression
    with ExpectsInputTypes
    with NullIntolerantShim {

  def inputExpressions: Seq[Expression]

  override def toString: String = s" **${this.getClass.getName}**  "

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[AbstractDataType] = Seq(GeographyUDT, GeographyUDT)

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions

  override final def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[Array[Byte]]
    if (leftArray == null) {
      null
    } else {
      val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[Array[Byte]]
      if (rightArray == null) {
        null
      } else {
        val leftGeog = S2GeographySerializer.deserialize(leftArray)
        val indexLeftGeog = new ShapeIndexGeography(leftGeog)
        val rightGeog = S2GeographySerializer.deserialize(rightArray)
        val indexRightGeog = new ShapeIndexGeography(rightGeog)
        try {
          evalGeom(indexLeftGeog, indexRightGeog)
        } catch {
          case e: Exception =>
            InferredExpression.throwExpressionInferenceException(
              getClass.getSimpleName,
              Seq(leftGeog, rightGeog),
              e)
        }
      }
    }
  }

  def evalGeom(leftGeog: S2Geography, rightGeog: S2Geography): Boolean
}

/**
 * Test if leftGeometry full contains rightGeometry
 *
 * @param inputExpressions
 */
case class ST_S2Contains(inputExpressions: Seq[Expression])
    extends ST_S2Predicate
    with CodegenFallback {

  override def evalGeom(leftGeog: S2Geography, rightGeog: S2Geography): Boolean = {
    Predicates.S2_contains(
      leftGeog.asInstanceOf[ShapeIndexGeography],
      rightGeog.asInstanceOf[ShapeIndexGeography],
      S2BooleanOperation.Options.DEFAULT)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if leftGeometry full intersects rightGeometry
 *
 * @param inputExpressions
 */
case class ST_S2Intersects(inputExpressions: Seq[Expression])
    extends ST_S2Predicate
    with CodegenFallback {

  override def evalGeom(leftGeog: S2Geography, rightGeog: S2Geography): Boolean = {
    Predicates.S2_intersects(
      leftGeog.asInstanceOf[ShapeIndexGeography],
      rightGeog.asInstanceOf[ShapeIndexGeography],
      S2BooleanOperation.Options.DEFAULT)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if leftGeometry is equal to rightGeometry
 *
 * @param inputExpressions
 */
case class ST_S2Equals(inputExpressions: Seq[Expression])
    extends ST_S2Predicate
    with CodegenFallback {

  override def evalGeom(leftGeog: S2Geography, rightGeog: S2Geography): Boolean = {
    Predicates.S2_equals(
      leftGeog.asInstanceOf[ShapeIndexGeography],
      rightGeog.asInstanceOf[ShapeIndexGeography],
      S2BooleanOperation.Options.DEFAULT)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
