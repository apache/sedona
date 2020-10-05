/*
 * FILE: DistanceJoinExec.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.geosparksql.strategy.join

import org.locationtech.jts.geom.Geometry
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.GeometrySerializer

// ST_Distance(left, right) <= radius
// radius can be literal or a computation over 'left'
case class DistanceJoinExec(left: SparkPlan,
                            right: SparkPlan,
                            leftShape: Expression,
                            rightShape: Expression,
                            radius: Expression,
                            intersects: Boolean,
                            extraCondition: Option[Expression] = None)
  extends BinaryExecNode
    with TraitJoinQueryExec
    with Logging {

  private val boundRadius = BindReferences.bindReference(radius, left.output)

  override def toSpatialRddPair(
                                 buildRdd: RDD[UnsafeRow],
                                 buildExpr: Expression,
                                 streamedRdd: RDD[UnsafeRow],
                                 streamedExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) =
    (toCircleRDD(buildRdd, buildExpr), toSpatialRdd(streamedRdd, streamedExpr))

  private def toCircleRDD(rdd: RDD[UnsafeRow], shapeExpression: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x => {
          val shape = GeometrySerializer.deserialize(shapeExpression.eval(x).asInstanceOf[ArrayData])
          val circle = new Circle(shape, boundRadius.eval(x).asInstanceOf[Double])
          circle.setUserData(x.copy)
          circle.asInstanceOf[Geometry]
        }
        }
        .toJavaRDD())
    spatialRdd
  }

}
