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

import org.apache.sedona.common.geography.GeographyKnnIndex
import org.apache.sedona.core.utils.SedonaConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, Predicate, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode

import scala.collection.JavaConverters._

/**
 * The objects of a geography KNN join as they are broadcast to the executors. The index over them
 * is built once per JVM, on first use.
 */
private[join] final class BroadcastGeographyKnnIndex(objects: Array[GeographyKnnInput])
    extends Serializable {
  @transient lazy val index: GeographyKnnIndex[UnsafeRow] = {
    val builder = GeographyKnnIndex.builder[UnsafeRow]()
    objects.foreach(input => builder.add(input.geography, input.row))
    builder.build()
  }
}

/**
 * Inner join on `ST_KNN(queryShape, objectShape, k)` over geography inputs, with one side
 * broadcast. Each query row is joined with its k nearest object rows, ranked by the distance that
 * `ST_Distance(geography, geography)` returns.
 *
 *   - When the object side is broadcast, the objects are collected and indexed once, and every
 *     query row searches that index.
 *   - When the query side is broadcast, every partition of objects is indexed and searched by
 *     every query, and the per-partition neighbours of a query are merged into its k nearest.
 *
 * @param queryShape
 *   the first argument of ST_KNN, a geography from the query side
 * @param objectShape
 *   the second argument of ST_KNN, a geography from the object side
 * @param broadcastSide
 *   the side of the join that is broadcast
 * @param querySide
 *   the side of the join that `queryShape` comes from
 * @param k
 *   the number of neighbours per query row
 * @param extraCondition
 *   the rest of the join condition, applied to the k nearest neighbours that were found
 */
case class BroadcastGeographyKNNJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    queryShape: Expression,
    objectShape: Expression,
    broadcastSide: JoinSide,
    querySide: JoinSide,
    k: Expression,
    extraCondition: Option[Expression] = None)
    extends SedonaBinaryExecNode
    with Logging {

  override def output: Seq[Attribute] = left.output ++ right.output

  override protected def doExecute(): RDD[InternalRow] = {
    val kValue = k.eval().asInstanceOf[Int]
    val includeTies = SedonaConf.fromActiveSession.isIncludeTieBreakersInKNNJoins
    val (queryPlan, objectPlan) = if (querySide == LeftSide) (left, right) else (right, left)
    val boundQueryShape = BindReferences.bindReference(queryShape, queryPlan.output)
    val boundObjectShape = BindReferences.bindReference(objectShape, objectPlan.output)
    val queryRows = queryPlan.execute().asInstanceOf[RDD[UnsafeRow]]
    val objectRows = objectPlan.execute().asInstanceOf[RDD[UnsafeRow]]

    if (broadcastSide == querySide) {
      logInfo("Geography KNN join: broadcasting the query side")
      joinWithBroadcastQueries(
        collectInputs(queryRows, boundQueryShape),
        objectRows,
        boundObjectShape,
        kValue,
        includeTies)
    } else {
      logInfo("Geography KNN join: broadcasting the object side")
      joinWithBroadcastObjects(
        queryRows,
        boundQueryShape,
        collectInputs(objectRows, boundObjectShape),
        kValue,
        includeTies)
    }
  }

  /** Collects the rows of the broadcast side that have a non-empty geography. */
  private def collectInputs(
      rows: RDD[UnsafeRow],
      boundShape: Expression): Array[GeographyKnnInput] =
    rows
      .mapPartitions(_.flatMap { row =>
        GeographyKnnInput.from(TraitJoinQueryBase.shapeToGeography(boundShape, row), row.copy())
      })
      .collect()

  private def joinWithBroadcastObjects(
      queryRows: RDD[UnsafeRow],
      boundQueryShape: Expression,
      objects: Array[GeographyKnnInput],
      kValue: Int,
      includeTies: Boolean): RDD[InternalRow] = {
    if (objects.isEmpty) {
      return sparkContext.emptyRDD[InternalRow]
    }
    val broadcastObjects = sparkContext.broadcast(new BroadcastGeographyKnnIndex(objects))
    val toOutput = outputRows
    queryRows.mapPartitions { rows =>
      val index = broadcastObjects.value.index
      toOutput(rows.flatMap { queryRow =>
        val query = TraitJoinQueryBase.shapeToGeography(boundQueryShape, queryRow)
        // The query row stays valid while its neighbours are consumed: the outer iterator only
        // advances once this inner iterator is exhausted.
        index
          .nearest(query, kValue, includeTies)
          .iterator
          .asScala
          .map(neighbor => (queryRow, neighbor.getItem))
      })
    }
  }

  private def joinWithBroadcastQueries(
      queries: Array[GeographyKnnInput],
      objectRows: RDD[UnsafeRow],
      boundObjectShape: Expression,
      kValue: Int,
      includeTies: Boolean): RDD[InternalRow] = {
    if (queries.isEmpty) {
      return sparkContext.emptyRDD[InternalRow]
    }
    val broadcastQueries = sparkContext.broadcast(queries)
    // The nearest neighbours of every query within each partition of objects, keyed by the
    // position of the query in the broadcast array.
    val partitionNeighbors = objectRows.mapPartitions { rows =>
      val builder = GeographyKnnIndex.builder[UnsafeRow]()
      rows.foreach { row =>
        builder.add(TraitJoinQueryBase.shapeToGeography(boundObjectShape, row), row.copy())
      }
      val index = builder.build()
      if (index.isEmpty) {
        Iterator.empty
      } else {
        broadcastQueries.value.iterator.zipWithIndex.flatMap { case (query, queryId) =>
          val neighbors = index.nearest(query.geography, kValue, includeTies)
          if (neighbors.isEmpty) None else Some((queryId, neighbors))
        }
      }
    }
    val toOutput = outputRows
    partitionNeighbors
      .reduceByKey((a, b) => GeographyKnnIndex.merge(a, b, kValue, includeTies))
      .mapPartitions { neighborsByQuery =>
        toOutput(neighborsByQuery.flatMap { case (queryId, neighbors) =>
          val queryRow = broadcastQueries.value(queryId).row
          neighbors.iterator.asScala.map(neighbor => (queryRow, neighbor.getItem))
        })
      }
  }

  /**
   * Returns a function, safe to ship to executors, that turns (query row, object row) pairs into
   * the output rows of this join: the two rows concatenated in left / right order, filtered by
   * the rest of the join condition.
   */
  private def outputRows: Iterator[(UnsafeRow, UnsafeRow)] => Iterator[InternalRow] = {
    val leftSchema = left.schema
    val rightSchema = right.schema
    val queriesOnLeft = querySide == LeftSide
    val joinOutput = output
    val residual = extraCondition
    matches => {
      val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
      val joined = matches.map { case (queryRow, objectRow) =>
        if (queriesOnLeft) joiner.join(queryRow, objectRow) else joiner.join(objectRow, queryRow)
      }
      residual match {
        case Some(condition) =>
          val boundCondition = Predicate.create(condition, joinOutput)
          joined.filter(row => boundCondition.eval(row))
        case None => joined
      }
    }
  }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
