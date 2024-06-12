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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.core.enums.IndexType
import org.apache.sedona.core.spatialRddTool.IndexBuilder

import scala.jdk.CollectionConverters._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaUnaryExecNode
import org.locationtech.jts.geom.Geometry

import java.util.Collections

case class SpatialIndexExec(
    child: SparkPlan,
    shape: Expression,
    indexType: IndexType,
    isRasterPredicate: Boolean,
    isGeography: Boolean,
    distance: Option[Expression] = None)
    extends SedonaUnaryExecNode
    with TraitJoinQueryBase
    with Logging {

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "SpatialIndex does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): Broadcast[T] = {
    val boundShape = BindReferences.bindReference(shape, child.output)
    val resultRaw = child.execute().asInstanceOf[RDD[UnsafeRow]].coalesce(1)
    val spatialRDD = distance match {
      case Some(distanceExpression) =>
        toExpandedEnvelopeRDD(
          resultRaw,
          boundShape,
          BindReferences.bindReference(distanceExpression, child.output),
          isGeography)
      case None =>
        if (isRasterPredicate) {
          toWGS84EnvelopeRDD(resultRaw, boundShape)
        } else {
          toSpatialRDD(resultRaw, boundShape)
        }
    }

    spatialRDD.buildIndex(indexType, false)
    val spatialIndexes = spatialRDD.indexedRawRDD.take(1).asScala
    val spatialIndex = if (spatialIndexes.nonEmpty) {
      spatialIndexes.head
    } else {
      // The broadcasted dataframe contains 0 partition. In this case, we should provide an empty spatial index.
      val indexBuilder = new IndexBuilder[Geometry](indexType)
      indexBuilder.call(Collections.emptyIterator()).next()
    }
    sparkContext.broadcast(spatialIndex).asInstanceOf[Broadcast[T]]
  }

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
