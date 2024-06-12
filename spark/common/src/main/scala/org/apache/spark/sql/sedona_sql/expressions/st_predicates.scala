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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.Column

object st_predicates extends DataFrameAPI {
  def ST_Contains(a: Column, b: Column): Column = wrapExpression[ST_Contains](a, b)
  def ST_Contains(a: String, b: String): Column = wrapExpression[ST_Contains](a, b)

  def ST_Crosses(a: Column, b: Column): Column = wrapExpression[ST_Crosses](a, b)
  def ST_Crosses(a: String, b: String): Column = wrapExpression[ST_Crosses](a, b)

  def ST_Disjoint(a: Column, b: Column): Column = wrapExpression[ST_Disjoint](a, b)
  def ST_Disjoint(a: String, b: String): Column = wrapExpression[ST_Disjoint](a, b)

  def ST_Equals(a: Column, b: Column): Column = wrapExpression[ST_Equals](a, b)
  def ST_Equals(a: String, b: String): Column = wrapExpression[ST_Equals](a, b)

  def ST_Intersects(a: Column, b: Column): Column = wrapExpression[ST_Intersects](a, b)
  def ST_Intersects(a: String, b: String): Column = wrapExpression[ST_Intersects](a, b)

  def ST_OrderingEquals(a: Column, b: Column): Column = wrapExpression[ST_OrderingEquals](a, b)
  def ST_OrderingEquals(a: String, b: String): Column = wrapExpression[ST_OrderingEquals](a, b)

  def ST_Overlaps(a: Column, b: Column): Column = wrapExpression[ST_Overlaps](a, b)
  def ST_Overlaps(a: String, b: String): Column = wrapExpression[ST_Overlaps](a, b)

  def ST_Touches(a: Column, b: Column): Column = wrapExpression[ST_Touches](a, b)
  def ST_Touches(a: String, b: String): Column = wrapExpression[ST_Touches](a, b)

  def ST_Relate(a: Column, b: Column): Column = wrapExpression[ST_Relate](a, b)
  def ST_Relate(a: String, b: String): Column = wrapExpression[ST_Relate](a, b)
  def ST_Relate(a: Column, b: Column, intersectionMatrix: Column): Column =
    wrapExpression[ST_Relate](a, b, intersectionMatrix)
  def ST_Relate(a: String, b: String, intersectionMatrix: String): Column =
    wrapExpression[ST_Relate](a, b, intersectionMatrix)

  def ST_RelateMatch(a: Column, b: Column): Column = wrapExpression[ST_RelateMatch](a, b)
  def ST_RelateMatch(a: String, b: String): Column = wrapExpression[ST_RelateMatch](a, b)

  def ST_Within(a: Column, b: Column): Column = wrapExpression[ST_Within](a, b)
  def ST_Within(a: String, b: String): Column = wrapExpression[ST_Within](a, b)

  def ST_Covers(a: Column, b: Column): Column = wrapExpression[ST_Covers](a, b)
  def ST_Covers(a: String, b: String): Column = wrapExpression[ST_Covers](a, b)

  def ST_CoveredBy(a: Column, b: Column): Column = wrapExpression[ST_CoveredBy](a, b)
  def ST_CoveredBy(a: String, b: String): Column = wrapExpression[ST_CoveredBy](a, b)
  def ST_DWithin(a: Column, b: Column, distance: Column): Column =
    wrapExpression[ST_DWithin](a, b, distance)
  def ST_DWithin(a: String, b: String, distance: Double): Column =
    wrapExpression[ST_DWithin](a, b, distance)
  def ST_DWithin(a: Column, b: Column, distance: Column, useSphere: Column): Column =
    wrapExpression[ST_DWithin](a, b, distance, useSphere)
  def ST_DWithin(a: String, b: String, distance: Double, useSphere: Boolean): Column =
    wrapExpression[ST_DWithin](a, b, distance, useSphere)
}
