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
package org.apache.sedona.sql

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.sedona_sql.UDT.{Box2DUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions.{ST_Box2D, ST_GeomFromBox2D}
import org.apache.spark.sql.sedona_sql.optimization.Box2DCastResolutionRule
import org.apache.spark.sql.types.LongType
import org.scalatest.funspec.AnyFunSpec

class Box2DCastResolutionRuleSuite extends AnyFunSpec {

  private val rule = new Box2DCastResolutionRule

  private def projectExprPlan(input: AttributeReference, expr: Expression): LogicalPlan = {
    val rel = LocalRelation(input)
    Project(Seq(Alias(expr, "out")()), rel)
  }

  describe("Box2DCastResolutionRule") {
    it("rewrites Cast(geometry-typed expression, Box2DUDT) into ST_Box2D") {
      val geomAttr = AttributeReference("g", GeometryUDT(), nullable = true)()
      val cast = Cast(geomAttr, Box2DUDT)
      val rewritten = rule(projectExprPlan(geomAttr, cast))
      val outExpr =
        rewritten.asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
      assert(outExpr.isInstanceOf[ST_Box2D])
      assert(outExpr.asInstanceOf[ST_Box2D].inputExpressions == Seq(geomAttr))
      assert(outExpr.dataType.isInstanceOf[Box2DUDT])
    }

    it("rewrites Cast(Box2D-typed expression, GeometryUDT) into ST_GeomFromBox2D") {
      val boxAttr = AttributeReference("b", Box2DUDT, nullable = true)()
      val cast = Cast(boxAttr, GeometryUDT())
      val rewritten = rule(projectExprPlan(boxAttr, cast))
      val outExpr =
        rewritten.asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
      assert(outExpr.isInstanceOf[ST_GeomFromBox2D])
      assert(outExpr.asInstanceOf[ST_GeomFromBox2D].inputExpressions == Seq(boxAttr))
      assert(outExpr.dataType.isInstanceOf[GeometryUDT])
    }

    it("leaves unrelated casts untouched") {
      val geomAttr = AttributeReference("g", GeometryUDT(), nullable = true)()
      val cast = Cast(Literal(1), LongType)
      val rewritten = rule(projectExprPlan(geomAttr, cast))
      val outExpr =
        rewritten.asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
      assert(outExpr.isInstanceOf[Cast])
    }
  }
}
