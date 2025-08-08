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
package org.apache.spark.sql.sedona_sql.expressions.geography

import org.apache.sedona.common.geography.Constructors
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression

/**
 * Return a Geography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKT.
 */
private[apache] case class ST_GeogFromWKT(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geogFromWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKT.
 */
private[apache] case class ST_GeogFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geogFromWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geography Collection from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKT.
 */
private[apache] case class ST_GeogCollFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geogCollFromText _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geography from a WKB string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
private[apache] case class ST_GeogFromWKB(inputExpressions: Seq[Expression])
    extends InferredExpression(
      Constructors.geogFromWKB(_: Array[Byte], _: Int),
      Constructors.geogFromWKB(_: String, _: Int),
      Constructors.geogFromWKB(_: Array[Byte]),
      Constructors.geogFromWKB(_: String)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2PointFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.pointFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2MPointFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.mPointFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2PolygonFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.polygonFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2MPolyFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.mPolyFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2LineFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.lineFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2MLineFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.mLineFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2LinestringFromText(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.lineStringFromText _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2LineFromWKB(inputExpressions: Seq[Expression])
    extends InferredExpression(
      Constructors.lineFromWKB(_: Array[Byte], _: Int),
      Constructors.lineFromWKB(_: String, _: Int),
      Constructors.lineFromWKB(_: Array[Byte]),
      Constructors.lineFromWKB(_: String)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a PointGeography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
case class ST_S2PointFromWKB(inputExpressions: Seq[Expression])
    extends InferredExpression(
      Constructors.pointFromWKB(_: Array[Byte], _: Int),
      Constructors.pointFromWKB(_: String, _: Int),
      Constructors.pointFromWKB(_: Array[Byte]),
      Constructors.pointFromWKB(_: String)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
