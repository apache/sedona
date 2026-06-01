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
import org.apache.spark.sql.sedona_sql.UDT.{Box3DUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions.ST_Box3D
import org.apache.spark.sql.sedona_sql.optimization.Box3DCastResolutionRule
import org.apache.spark.sql.types.LongType
import org.scalatest.funspec.AnyFunSpec

class Box3DCastResolutionRuleSuite extends AnyFunSpec {

  private val rule = new Box3DCastResolutionRule

  private def projectExprPlan(input: AttributeReference, expr: Expression): LogicalPlan = {
    val rel = LocalRelation(input)
    Project(Seq(Alias(expr, "out")()), rel)
  }

  describe("Box3DCastResolutionRule") {
    it("rewrites Cast(geometry-typed expression, Box3DUDT) into ST_Box3D") {
      val geomAttr = AttributeReference("g", GeometryUDT(), nullable = true)()
      val cast = Cast(geomAttr, Box3DUDT)
      val rewritten = rule(projectExprPlan(geomAttr, cast))
      val outExpr =
        rewritten.asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
      assert(outExpr.isInstanceOf[ST_Box3D])
      assert(outExpr.asInstanceOf[ST_Box3D].inputExpressions == Seq(geomAttr))
      assert(outExpr.dataType.isInstanceOf[Box3DUDT])
    }

    it("leaves Cast(Box3D-typed expression, GeometryUDT) untouched (inverse cast not in scope)") {
      val boxAttr = AttributeReference("b", Box3DUDT, nullable = true)()
      val cast = Cast(boxAttr, GeometryUDT())
      val rewritten = rule(projectExprPlan(boxAttr, cast))
      val outExpr =
        rewritten.asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
      assert(outExpr.isInstanceOf[Cast])
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
