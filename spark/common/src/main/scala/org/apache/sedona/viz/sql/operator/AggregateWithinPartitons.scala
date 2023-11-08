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

import org.apache.sedona.viz.sql.utils.Conf
import org.apache.sedona.viz.utils.Pixel
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object AggregateWithinPartitons {
  /**
    * Run aggregation within each partition without incurring a data shuffle. Currently support three aggregates, sum, count, avg.
    * If the aggregate func is count, this function doesn't require a value column. If the aggregate func is sum and avg, it will require
    * a value column. They are same as the regular aggregation SQL
    *
    * SELECT pixel, COUNT(*)
    * FROM t GROUP BY pixel
    *
    * SELECT pixel, AVG(weight)
    * FROM t GROUP BY pixel
    *
    * SELECT pixel, AVG(weight)
    * FROM t GROUP BY pixel
    *
    * @param dataFrame
    * @param keyCol   GroupBy key
    * @param valueCol Aggregate value
    * @param aggFunc  Aggregate function
    * @return
    */
  def apply(dataFrame: DataFrame, keyCol: String, valueCol: String, aggFunc: String): DataFrame = {
    // Keep keycol, valuecol, primary partition id, secondary partition id
    var formattedDf: DataFrame = null
    // If the aggFunc is count, we don't need to make sure the valueCol exists because we just simply count the number of rows
    if (aggFunc.equalsIgnoreCase("count")) formattedDf = dataFrame.select(keyCol, Conf.PrimaryPID, Conf.SecondaryPID).withColumn(valueCol, lit(0.0))
    else formattedDf = dataFrame.select(keyCol, Conf.PrimaryPID, Conf.SecondaryPID, valueCol)

    // formattedDf.show()

    val aggRdd = formattedDf.rdd.mapPartitions(iterator => {
      var aggregator = new mutable.HashMap[Pixel, Tuple4[Double, Double, String, String]]()
      while (iterator.hasNext) {
        val cursorRow = iterator.next()
        val cursorKey = cursorRow.getAs[Pixel](keyCol)
        val cursorValue = cursorRow.getAs[Double](valueCol)
        val currentAggregate = aggregator.getOrElse(cursorKey, Tuple4(0.0, 0.0, "", ""))
        // Update the aggregator values, partition ids are appended directly
        aggregator.update(
          cursorKey,
          Tuple4(
            currentAggregate._1 + cursorValue,
            currentAggregate._2 + 1,
            cursorRow.getAs[String](Conf.PrimaryPID),
            cursorRow.getAs[String](Conf.SecondaryPID)))
      }
      var result = new ArrayBuffer[Row]()
      aggFunc match {
        case "sum" => aggregator.foreach(f => {
          result += Row(f._1, f._2._3, f._2._4, f._2._1)
        })
        case "count" => aggregator.foreach(f => {
          result += Row(f._1, f._2._3, f._2._4, f._2._2)
        })
        case "avg" => aggregator.foreach(f => {
          result += Row(f._1, f._2._3, f._2._4, f._2._1 / f._2._2)
        })
        case _ => throw new IllegalArgumentException("[Sedona-Viz][SQL] Unsupported aggregate func type. Only sum, count, avg are supported.")
      }
      result.iterator
    })
    formattedDf.sparkSession.createDataFrame(aggRdd, formattedDf.schema)
  }
}
