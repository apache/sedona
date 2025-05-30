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
package org.apache.spark.sql.sedona_sql.io.stac

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.util.SerializableConfiguration

/**
 * The `StacScanBuilder` class represents the builder for creating a `Scan` instance in the
 * SpatioTemporal Asset Catalog (STAC) data source.
 *
 * This class is responsible for assembling the scan operation for reading STAC data. It acts as a
 * bridge between Spark's data source API and the specific implementation of the STAC data read
 * operation.
 */
class StacScanBuilder(
    stacCollectionJson: String,
    opts: Map[String, String],
    broadcastConf: Broadcast[SerializableConfiguration])
    extends ScanBuilder {

  /**
   * Builds and returns a `Scan` instance. The `Scan` defines the schema and batch reading methods
   * for STAC data.
   *
   * @return
   *   A `Scan` instance that defines how to read STAC data.
   */
  override def build(): Scan = new StacScan(stacCollectionJson, opts, broadcastConf)
}
