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
import org.apache.spark.sql.sedona_sql.UDT.{Box3DUDT, GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions.ST_Box3D

/**
 * Analyzer rule that resolves Catalyst casts from Geometry to Box3D. Spark's `Cast.canCast`
 * returns `false` for arbitrary UDT-to-UDT casts, so without this rule the analyzer would reject
 * `CAST(geom AS box3d)`. We rewrite during analysis (before `CheckAnalysis`) so the downstream
 * optimizer and codegen path see the expression tree of an ordinary Sedona expression:
 *
 *   - `CAST(geom AS box3d)` → `ST_Box3D(geom)` (planar 3D bounding box of the geometry;
 *     geometries without a Z dimension fold into `zmin = zmax = 0` per PostGIS)
 *
 * The inverse direction (`CAST(box3d AS geometry)`) is intentionally deferred until Box3D has a
 * `ST_GeomFromBox3D` counterpart and a concrete consumer has driven the choice of output geometry
 * shape. Implicit type coercion is also out of scope here; it requires hooking into Catalyst's
 * type coercion rules and is tracked separately.
 */
class Box3DCastResolutionRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case c: Cast
        if c.child.resolved
          && c.child.dataType.isInstanceOf[GeometryUDT]
          && c.dataType.isInstanceOf[Box3DUDT] =>
      ST_Box3D(Seq(c.child))
  }
}
