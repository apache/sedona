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
package org.apache.sedona.viz.sql.operator

import org.apache.sedona.core.spatialPartitioning.QuadtreePartitioning
import org.apache.sedona.core.spatialPartitioning.quadtree.QuadRectangle
import org.apache.sedona.viz.sql.utils.{Conf, LineageDecoder}
import org.apache.sedona.viz.utils.Pixel
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.{DataFrame, Row}
import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

object VizPartitioner {

  /**
    * Partition the data frame to many pieces. Each partition has two partition ids.
    * Primary id is the uniform map tile id and secondary id is the non-uniformed partition id.
    * The generated DataFrame guarantees that rows with the same secondary ids are in the partition.
    * But each partition may contain rows with different secondary ids.
    *
    * @param dataFrame
    * @param zoomLevel
    * @param spatialColName
    * @param boundary
    * @return
    */
  def apply(dataFrame: DataFrame, zoomLevel: Int, spatialColName: String, boundary: Envelope): DataFrame = {
    val samples = dataFrame.sample(false, 0.01).select(spatialColName).collect().map(f => f.getAs[Pixel](0).getEnvelopeInternal).toList.asJava
    val numberParts = Math.pow(4, zoomLevel * 1.0).intValue()

    // Prepare the secondary partitioner
    var secondaryPartitioner = new QuadtreePartitioning(samples, boundary, numberParts, zoomLevel)
    var secondaryPartitionTree = secondaryPartitioner.getPartitionTree
    secondaryPartitionTree.assignPartitionLineage()
    // Drop the spatial objects inside the tree
    secondaryPartitionTree.dropElements()
    var existPrimaryIdCol = false
    var existSecondaryIdCol = false
    dataFrame.schema.fields.foreach(f => {
      if (f.name.equalsIgnoreCase(Conf.PrimaryPID)) existPrimaryIdCol = true
      if (f.name.equalsIgnoreCase(Conf.SecondaryPID)) existPrimaryIdCol = true
    })
    var partitionedDf = dataFrame
    // Append new primary and secondary partition IDs if they don't exist
    if (!existPrimaryIdCol && !existSecondaryIdCol) partitionedDf = dataFrame.withColumn(Conf.PrimaryPID, lit("")).withColumn(Conf.SecondaryPID, lit(""))
    val rddWithPID = partitionedDf.rdd.mapPartitions(iterator => {
      var list = new ArrayBuffer[Row]()
      while (iterator.hasNext) {
        val cursorRow = iterator.next()
        val primaryIdPos = cursorRow.fieldIndex(Conf.PrimaryPID)
        val secondIdPos = cursorRow.fieldIndex(Conf.SecondaryPID)
        val geometry = cursorRow.getAs[Geometry](spatialColName).getEnvelopeInternal
        // This is to find all seconary ids
        val matchedZones = secondaryPartitionTree.findZones(new QuadRectangle(geometry)).asScala
        matchedZones.foreach(secondaryZone => {
          var currentValues = cursorRow.toSeq.toBuffer
          // Assign secondary id
          currentValues(secondIdPos) = LineageDecoder(secondaryZone.lineage)
          // Assign primary id
          currentValues(primaryIdPos) = LineageDecoder(secondaryZone.lineage.take(zoomLevel))
          list += Row.fromSeq(currentValues.toSeq)
        })
      }
      list.iterator
    })
    val dfWithPID = partitionedDf.sparkSession.createDataFrame(rddWithPID, partitionedDf.schema)
    dfWithPID.repartition(dfWithPID.select(Conf.SecondaryPID).distinct().count().toInt, expr(Conf.SecondaryPID))
    //      .sortWithinPartitions(Conf.SecondaryPID)
  }
}
