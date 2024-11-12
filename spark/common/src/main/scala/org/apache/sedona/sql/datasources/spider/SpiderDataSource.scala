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
package org.apache.sedona.sql.datasources.spider

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import java.util

/**
 * DataSource for generating random geometric data. The idea and algorithms of this data source
 * comes from this following publication: Puloma Katiyar, Tin Vu, Sara Migliorini, Alberto
 * Belussi, Ahmed Eldawy. "SpiderWeb: A Spatial Data Generator on the Web", ACM SIGSPATIAL 2020,
 * Seattle, WA
 */
class SpiderDataSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "spider"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    SpiderTable.SCHEMA
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val opts = new CaseInsensitiveStringMap(properties)
    var numPartitions = opts.getInt("numPartitions", -1)
    if (numPartitions < 0) {
      numPartitions = SparkSession.active.sparkContext.defaultParallelism
    }
    new SpiderTable(
      numRows = opts.getOrDefault("cardinality", opts.getOrDefault("n", "100")).toLong,
      numPartitions = numPartitions,
      seed = opts.getLong("seed", System.currentTimeMillis()),
      distribution = opts.getOrDefault("distribution", "uniform"),
      transform = AffineTransform(
        translateX = opts.getDouble("translateX", 0),
        translateY = opts.getDouble("translateY", 0),
        scaleX = opts.getDouble("scaleX", 1),
        scaleY = opts.getDouble("scaleY", 1),
        skewX = opts.getDouble("skewX", 0),
        skewY = opts.getDouble("skewY", 0)),
      opts)
  }
}
