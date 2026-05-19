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
package org.apache.spark.sql.sedona_sql.optimization

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sedona_sql.UDT.{Box2DUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions.{ST_Box2D, ST_GeomFromBox2D}

/**
 * Analyzer rule that resolves Catalyst casts between Sedona UDTs that Spark's stock cast resolver
 * does not handle. Specifically:
 *
 *   - `CAST(geom AS box2d)` → `ST_Box2D(geom)` (planar bounding box of the geometry)
 *   - `CAST(box AS geometry)` → `ST_GeomFromBox2D(box)` (rectangular polygon from a Box2D)
 *
 * Spark's `Cast.canCast` returns `false` for arbitrary UDT-to-UDT casts, so without this rule the
 * analyzer would reject the cast. We rewrite during analysis (before `CheckAnalysis`) so the
 * downstream optimizer and codegen path see the expression tree of an ordinary Sedona expression.
 *
 * Implicit type coercion (e.g. passing a Geometry into a Box2D-typed function argument without an
 * explicit cast) is intentionally out of scope here; it requires hooking into Catalyst's type
 * coercion rules and is tracked separately.
 */
class Box2DCastResolutionRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case c: Cast
        if c.child.resolved
          && c.child.dataType.isInstanceOf[GeometryUDT]
          && c.dataType.isInstanceOf[Box2DUDT] =>
      ST_Box2D(Seq(c.child))

    case c: Cast
        if c.child.resolved
          && c.child.dataType.isInstanceOf[Box2DUDT]
          && c.dataType.isInstanceOf[GeometryUDT] =>
      ST_GeomFromBox2D(Seq(c.child))
  }
}
