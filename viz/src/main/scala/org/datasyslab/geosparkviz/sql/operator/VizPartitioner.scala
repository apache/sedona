/**
  * FILE: VizPartitioner
  * Copyright (c) 2015 - 2019 GeoSpark Development Team
  *
  * MIT License
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package org.datasyslab.geosparkviz.sql.operator

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle
import org.datasyslab.geosparkviz.sql.utils.{Conf, LineageDecoder}
import org.datasyslab.geosparkviz.utils.Pixel

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object VizPartitioner {

  /**
    * Partition the data frame to many pieces. Each partition has two partition ids. Primary id is the uniform map tile id and secondary id is the non-uniformed partition id.
    * The generated DataFrame guarantees that rows with the same secondary ids are in the partition. But each partition may contain rows with different secondary ids.
    * @param dataFrame
    * @param zoomLevel
    * @param spatialColName
    * @param boundary
    * @return
    */
  def apply(dataFrame: DataFrame, zoomLevel:Int, spatialColName: String, boundary: Envelope): DataFrame = {
    val samples = dataFrame.sample(0.01).select(spatialColName).collect().map(f => f.getAs[Pixel](0).getEnvelopeInternal).toList.asJava
    val numberParts = Math.pow(4, zoomLevel*1.0).intValue()

    // Prepare the secondary partitioner
    var secondaryPartitioner = new QuadtreePartitioning(samples, boundary, numberParts, zoomLevel)
    var secondaryPartitionTree = secondaryPartitioner.getPartitionTree
    secondaryPartitionTree.assignPartitionLineage()
    // Drop the spatial objects inside the tree
    secondaryPartitionTree.dropElements()
    var existPrimaryIdCol = false
    var existSecondaryIdCol = false
    dataFrame.schema.fields.foreach(f=>{
      if (f.name.equalsIgnoreCase(Conf.PrimaryPID)) existPrimaryIdCol=true
      if (f.name.equalsIgnoreCase(Conf.SecondaryPID)) existPrimaryIdCol=true
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
        matchedZones.foreach(secondaryZone=>{
          var currentValues = cursorRow.toSeq.toBuffer
          // Assign secondary id
          currentValues(secondIdPos) = LineageDecoder(secondaryZone.lineage)
          // Assign primary id
          currentValues(primaryIdPos) = LineageDecoder(secondaryZone.lineage.take(zoomLevel))
          list+=Row.fromSeq(currentValues)
        })
      }
      list.iterator
    })
    val dfWithPID = partitionedDf.sparkSession.createDataFrame(rddWithPID, partitionedDf.schema)
    dfWithPID.repartition(dfWithPID.select(Conf.SecondaryPID).distinct().count().toInt, expr(Conf.SecondaryPID))
//      .sortWithinPartitions(Conf.SecondaryPID)
  }
}