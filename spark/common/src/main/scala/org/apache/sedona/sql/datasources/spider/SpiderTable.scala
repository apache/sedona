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

import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SpiderTable(
    numRows: Long,
    numPartitions: Int,
    seed: Long,
    distribution: String,
    transform: AffineTransform,
    opts: CaseInsensitiveStringMap)
    extends Table
    with SupportsRead {

  override def name(): String = "spider"

  override def schema(): StructType = SpiderTable.SCHEMA

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new SpiderScanBuilder(distribution, numPartitions, numRows, opts, seed, transform)
  }
}

object SpiderTable {
  val SCHEMA: StructType = StructType(
    Seq(StructField("id", LongType), StructField("geometry", GeometryUDT())))
}
