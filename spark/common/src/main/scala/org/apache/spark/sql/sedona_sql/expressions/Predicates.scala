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

import org.apache.sedona.common.Predicates
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType}
import org.locationtech.jts.geom.Geometry

abstract class ST_Predicate extends Expression
  with FoldableExpression
  with ExpectsInputTypes
  with NullIntolerant {

  def inputExpressions: Seq[Expression]

  override def toString: String = s" **${this.getClass.getName}**  "

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT, GeometryUDT)

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
        val leftGeometry = GeometrySerializer.deserialize(leftArray)
        val rightGeometry = GeometrySerializer.deserialize(rightArray)
        evalGeom(leftGeometry, rightGeometry)
      }
    }
  }

  def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean
}

/**
  * Test if leftGeometry full contains rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Contains(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.contains(leftGeometry, rightGeometry)
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
case class ST_Intersects(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.intersects(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Test if leftGeometry is full within rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Within(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.within(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Test if leftGeometry covers rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Covers(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.covers(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Test if leftGeometry is covered by rightGeometry
  *
  * @param inputExpressions
  */
case class ST_CoveredBy(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.coveredBy(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Test if leftGeometry crosses rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Crosses(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.crosses(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


/**
  * Test if leftGeometry overlaps rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Overlaps(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.overlaps(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Test if leftGeometry touches rightGeometry
  *
  * @param inputExpressions
  */
case class ST_Touches(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.touches(leftGeometry, rightGeometry)
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
case class ST_Equals(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    // Returns GeometryCollection object
    Predicates.equals(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if leftGeometry is disjoint from rightGeometry
 *
 * @param inputExpressions
 */
case class ST_Disjoint(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.disjoint(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if leftGeometry is order equal to rightGeometry
 *
 * @param inputExpressions
 */
case class ST_OrderingEquals(inputExpressions: Seq[Expression])
  extends ST_Predicate with CodegenFallback {

  override def evalGeom(leftGeometry: Geometry, rightGeometry: Geometry): Boolean = {
    Predicates.orderingEquals(leftGeometry, rightGeometry)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
