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

import org.apache.sedona.core.enums.JoinSpartitionDominantSide
import org.apache.sedona.core.spatialOperator.{JoinQuery, SpatialPredicate}
import org.apache.sedona.core.spatialOperator.JoinQuery.JoinParams
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, GenericInternalRow, Predicate, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry

/**
 * Non-broadcast left outer spatial join for range predicates (ST_Contains, ST_Intersects, etc.).
 *
 * Executes an optimized partitioned inner spatial join, then finds unmatched left rows via an
 * equi-join on row IDs and appends them with null-padded right columns.
 *
 * @param left
 *   left side of the join (all rows preserved)
 * @param right
 *   right side of the join (null-padded when no match)
 * @param leftShape
 *   expression for the first argument of spatialPredicate
 * @param rightShape
 *   expression for the second argument of spatialPredicate
 * @param spatialPredicate
 *   spatial predicate as join condition
 * @param extraCondition
 *   extra join condition other than spatialPredicate
 */
case class RangeLeftOuterJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftShape: Expression,
    rightShape: Expression,
    spatialPredicate: SpatialPredicate,
    extraCondition: Option[Expression] = None)
    extends SedonaBinaryExecNode
    with TraitJoinQueryExec
    with Logging {

  override def output: Seq[Attribute] =
    left.output ++ right.output.map(_.withNullability(true))

  override protected def doExecute(): RDD[InternalRow] = {
    val boundLeftShape = BindReferences.bindReference(leftShape, left.output)
    val boundRightShape = BindReferences.bindReference(rightShape, right.output)

    val leftResultsRaw = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResultsRaw = right.execute().asInstanceOf[RDD[UnsafeRow]]

    val sedonaConf = SedonaConf.fromActiveSession

    // Step 1: Assign unique Long IDs to left rows (narrow transformation, no shuffle).
    // Cached because it's consumed twice: once for the spatial join, once for subtractByKey.
    val leftWithId: RDD[(UnsafeRow, Long)] = leftResultsRaw.zipWithUniqueId().cache()

    // Step 2: Convert to SpatialRDDs
    val leftShapes = toSpatialRDDWithId(leftWithId, boundLeftShape)
    val rightShapes = toSpatialRDD(rightResultsRaw, boundRightShape)

    // Step 3: Spatial partitioning (reuse logic from TraitJoinQueryExec)
    if (sedonaConf.getJoinApproximateTotalCount == -1) {
      if (sedonaConf.getJoinSpartitionDominantSide == JoinSpartitionDominantSide.LEFT) {
        leftShapes.analyze()
      } else {
        rightShapes.analyze()
      }
    }
    log.info("[SedonaSQL] Number of partitions on the left: " + leftResultsRaw.partitions.size)
    log.info("[SedonaSQL] Number of partitions on the right: " + rightResultsRaw.partitions.size)

    var numPartitions = -1
    try {
      if (sedonaConf.getJoinSpartitionDominantSide == JoinSpartitionDominantSide.LEFT) {
        if (sedonaConf.getFallbackPartitionNum != -1) {
          numPartitions = sedonaConf.getFallbackPartitionNum
        } else {
          numPartitions = joinPartitionNumOptimizer(
            leftShapes.rawSpatialRDD.partitions.size(),
            rightShapes.rawSpatialRDD.partitions.size(),
            leftShapes.approximateTotalCount)
        }
        doSpatialPartitioning(leftShapes, rightShapes, numPartitions, sedonaConf)
      } else {
        if (sedonaConf.getFallbackPartitionNum != -1) {
          numPartitions = sedonaConf.getFallbackPartitionNum
        } else {
          numPartitions = joinPartitionNumOptimizer(
            rightShapes.rawSpatialRDD.partitions.size(),
            leftShapes.rawSpatialRDD.partitions.size(),
            rightShapes.approximateTotalCount)
        }
        doSpatialPartitioning(rightShapes, leftShapes, numPartitions, sedonaConf)
      }
    } catch {
      case e: IllegalArgumentException =>
        print(e.getMessage)
        if (sedonaConf.getJoinSpartitionDominantSide == JoinSpartitionDominantSide.LEFT) {
          numPartitions = sedonaConf.getFallbackPartitionNum
          doSpatialPartitioning(leftShapes, rightShapes, numPartitions, sedonaConf)
        } else {
          numPartitions = sedonaConf.getFallbackPartitionNum
          doSpatialPartitioning(rightShapes, leftShapes, numPartitions, sedonaConf)
        }
    }

    // Step 4: Execute inner spatial join with dedup
    val joinParams = new JoinParams(
      sedonaConf.getUseIndex,
      spatialPredicate,
      sedonaConf.getIndexType,
      sedonaConf.getJoinBuildSide)

    val matchesRDD: RDD[(Geometry, Geometry)] =
      (leftShapes.spatialPartitionedRDD, rightShapes.spatialPartitionedRDD) match {
        case (null, null) =>
          sparkContext.parallelize(Seq[(Geometry, Geometry)]())
        case _ => JoinQuery.spatialJoin(leftShapes, rightShapes, joinParams).rdd
      }

    // Step 5: Extract matched pairs as (leftId, (leftRow, rightRow))
    val leftSchema = left.schema
    val rightSchema = right.schema
    val matchedWithId: RDD[(Long, (UnsafeRow, UnsafeRow))] = matchesRDD.map {
      case (leftGeom, rightGeom) =>
        val (leftId, leftRow) = leftGeom.getUserData.asInstanceOf[(Long, UnsafeRow)]
        val rightRow = rightGeom.getUserData.asInstanceOf[UnsafeRow]
        (leftId, (leftRow, rightRow))
    }

    // Step 6: Apply extra condition filter on matched pairs
    // This must happen before computing unmatched set (SQL outer join semantics:
    // a left row that matches spatially but fails the extra condition appears with nulls)
    val filteredMatches: RDD[(Long, (UnsafeRow, UnsafeRow))] = extraCondition match {
      case Some(condition) =>
        matchedWithId.mapPartitions { iter =>
          val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
          val boundCondition = Predicate.create(condition, output)
          iter.filter { case (_, (leftRow, rightRow)) =>
            boundCondition.eval(joiner.join(leftRow, rightRow))
          }
        }
      case None => matchedWithId
    }

    // Step 7: Build matched output rows, preserving matched left IDs for step 8.
    // Cached because it's consumed twice: once for matched output, once for matchedIds.
    val matchedWithJoinedRow: RDD[(Long, UnsafeRow)] = filteredMatches
      .mapPartitions { iter =>
        val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
        iter.map { case (id, (leftRow, rightRow)) => (id, joiner.join(leftRow, rightRow)) }
      }
      .cache()

    // Step 8: Find unmatched left rows via subtractByKey (one shuffle on Long keys)
    val matchedIds: RDD[(Long, Null)] =
      matchedWithJoinedRow.map { case (id, _) => (id, null) }
    val allLeftById: RDD[(Long, UnsafeRow)] = leftWithId.map { case (row, id) => (id, row) }
    val unmatchedLeft: RDD[(Long, UnsafeRow)] = allLeftById.subtractByKey(matchedIds)

    // Step 9: Build unmatched output rows with null-padded right columns
    val rightOutputLength = right.output.length
    val unmatchedOutput: RDD[InternalRow] = unmatchedLeft.mapPartitions { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
      val nullRow = new GenericInternalRow(rightOutputLength)
      val nullUnsafeRow = UnsafeProjection.create(rightSchema).apply(nullRow)
      iter.map { case (_, leftRow) => joiner.join(leftRow, nullUnsafeRow) }
    }

    // Step 10: Union matched and unmatched
    val matchedOutput: RDD[InternalRow] = matchedWithJoinedRow.map(_._2)
    matchedOutput.union(unmatchedOutput)
  }

  private def toSpatialRDDWithId(
      rdd: RDD[(UnsafeRow, Long)],
      shapeExpression: Expression): SpatialRDD[Geometry] = {
    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { case (row, id) =>
          val shape =
            GeometrySerializer.deserialize(shapeExpression.eval(row).asInstanceOf[Array[Byte]])
          shape.setUserData((id, row.copy()))
          shape
        }
        .toJavaRDD())
    spatialRdd
  }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
