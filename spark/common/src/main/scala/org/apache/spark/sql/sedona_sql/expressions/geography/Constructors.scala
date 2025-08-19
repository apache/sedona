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

import org.apache.sedona.common.S2Geography.Geography
import org.apache.sedona.common.geography.Constructors
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sedona_sql.UDT.GeographyUDT
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.{InferrableFunction, InferredExpression}
import org.locationtech.jts.geom.Geometry

/**
 * Return a Geography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geography string and a srid. The string format must be WKT.
 */
private[apache] case class ST_GeogFromWKT(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geogFromWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geography from a EWKT string
 *
 * @param inputExpressions
 *   This function takes a geography string. The string format must be EWKT.
 */
private[apache] case class ST_GeogFromEWKT(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geogFromEWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geography from a WKT string
 *
 * @param inputExpressions
 *   This function takes a geography string and a srid. The string format must be WKT.
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
 *   This function takes a geography string and a srid. The string format must be WKT.
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
 *   This function takes a geography string and a srid. The string format must be WKB binary array
 *   / string.
 */
private[apache] case class ST_GeogFromWKB(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geogFromWKB(_: Array[Byte], _: Int)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geography from a EWKB string
 *
 * @param inputExpressions
 *   This function takes a geometry string and a srid. The string format must be WKB binary array
 *   / string.
 */
private[apache] case class ST_GeogFromEWKB(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geogFromWKB(_: Array[Byte])) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geography from a GeoHash string
 *
 * @param inputExpressions
 *   This function takes a geography geohash
 */
private[apache] case class ST_GeogFromGeoHash(inputExpressions: Seq[Expression])
    extends InferredExpression(InferrableFunction.allowRightNull(Constructors.geogFromGeoHash)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a Geography
 *
 * @param inputExpressions
 *   This function takes a geography object.
 */
private[apache] case class ST_GeogToGeometry(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geogToGeometry(_: Geography)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geography from a Geometry
 *
 * @param inputExpressions
 *   This function takes a geometry object.
 */
private[apache] case class ST_GeomToGeography(inputExpressions: Seq[Expression])
    extends InferredExpression(Constructors.geomToGeography(_: Geometry)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
