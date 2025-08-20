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
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.execution.datasource.stac.TemporalFilter
import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetSpatialFilter
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.{getFullCollectionUrl, inferStacSchema}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class StacScan(
    stacCollectionJson: String,
    opts: Map[String, String],
    broadcastConf: Broadcast[SerializableConfiguration])
    extends Scan
    with SupportsMetadata {

  // The spatial filter to be pushed down to the data source
  var spatialFilter: Option[GeoParquetSpatialFilter] = None

  // The temporal filter to be pushed down to the data source
  var temporalFilter: Option[TemporalFilter] = None

  var limit: Option[Int] = None

  /**
   * Returns the schema of the data to be read.
   *
   * The schema is statically defined in the `StacTable` object.
   */
  override def readSchema(): StructType = {
    val url = opts.get("path")
    val service = opts.get("service")

    if (url == null && service == null) {
      throw new IllegalArgumentException("Either 'path' or 'service' must be provided")
    }

    inferStacSchema(opts)
  }

  /**
   * Returns a `Batch` instance for reading the data in batch mode.
   *
   * The `StacBatch` class provides the implementation for the batch reading.
   *
   * @return
   *   A `Batch` instance for batch-based data processing.
   */
  override def toBatch: Batch = {
    val stacCollectionUrl = getFullCollectionUrl(opts)
    StacBatch(
      broadcastConf,
      stacCollectionUrl,
      stacCollectionJson,
      readSchema(),
      opts,
      spatialFilter,
      temporalFilter,
      limit)
  }

  /**
   * Sets the spatial predicates to be pushed down to the data source.
   *
   * @param combinedSpatialFilter
   *   The combined spatial filter to be pushed down.
   */
  def setSpatialPredicates(combinedSpatialFilter: GeoParquetSpatialFilter) = {
    spatialFilter = Some(combinedSpatialFilter)
  }

  /**
   * Sets the temporal predicates to be pushed down to the data source.
   *
   * @param combineTemporalFilter
   *   The combined temporal filter to be pushed down.
   */
  def setTemporalPredicates(combineTemporalFilter: TemporalFilter) = {
    temporalFilter = Some(combineTemporalFilter)
  }

  /**
   * Sets the limit on the number of items to be read.
   *
   * @param n
   *   The limit on the number of items to be read.
   */
  def setLimit(n: Int) = {
    limit = Some(n)
  }

  /**
   * Returns metadata about the data to be read.
   *
   * The metadata includes information about the pushed filters.
   *
   * @return
   *   A map of metadata key-value pairs.
   */
  override def getMetaData(): Map[String, String] = {
    Map(
      "PushedSpatialFilters" -> spatialFilter.map(_.toString).getOrElse("None"),
      "PushedTemporalFilters" -> temporalFilter.map(_.toString).getOrElse("None"),
      "Pushedimit" -> limit.map(_.toString).getOrElse("None"))
  }

  /**
   * Returns a description of the data to be read.
   *
   * The description includes the metadata information.
   *
   * @return
   *   A string description of the data to be read.
   */
  override def description(): String = {
    super.description() + " " + getMetaData().mkString(", ")
  }
}
