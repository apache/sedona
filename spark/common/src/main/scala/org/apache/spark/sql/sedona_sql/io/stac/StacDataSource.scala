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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.io.geojson.GeoJSONUtils
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.{inferStacSchema, updatePropertiesPromotedSchema}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * The `StacDataSource` class is responsible for enabling the reading of SpatioTemporal Asset
 * Catalogs (STAC) as tables in Apache Spark. It allows integrating geospatial metadata from local
 * or remote STAC catalog sources into Spark for processing.
 *
 * This class implements Apache Spark's `TableProvider` interface to define how STAC data sources
 * are converted into Spark tables, and the `DataSourceRegister` interface to provide a custom
 * short name for easier data source loading.
 */
class StacDataSource() extends TableProvider with DataSourceRegister {

  // Cache to store inferred schemas
  private val schemaCache = new ConcurrentHashMap[Map[String, String], StructType]()

  /**
   * Returns the short name of this data source, which can be used in Spark SQL queries for
   * loading the data source. For example:
   *
   * `spark.read.format("stac").load(...)`
   *
   * @return
   *   The string identifier for this data source, "stac".
   */
  override def shortName(): String = "stac"

  /**
   * Infers and returns the schema of the STAC data source. This implementation checks if a local
   * cache of the processed STAC collection exists. If not, it processes the STAC collection and
   * saves it as a GeoJson file. The schema is then inferred from this GeoJson file.
   *
   * @param opts
   *   Mapping of data source options, which should include either 'url' or 'service'.
   * @return
   *   The inferred schema of the STAC data source table.
   * @throws IllegalArgumentException
   *   If neither 'url' nor 'service' are provided.
   */
  override def inferSchema(opts: CaseInsensitiveStringMap): StructType = {
    val optsMap = opts.asCaseSensitiveMap().asScala.toMap

    // Check if the schema is already cached
    val fullSchema = schemaCache.computeIfAbsent(optsMap, _ => inferStacSchema(optsMap))
    val updatedGeometrySchema = GeoJSONUtils.updateGeometrySchema(fullSchema, GeometryUDT())
    updatePropertiesPromotedSchema(updatedGeometrySchema)
  }

  /**
   * Provides a table implementation for the STAC data source based on the input schema and
   * configuration properties. This method supports loading STAC catalogs either from a local file
   * system or from a remote HTTP/HTTPS endpoint.
   *
   * @param schema
   *   The schema of the table, ignored as the schema is pre-defined.
   * @param partitioning
   *   Unused, but represents potential transformations (partitioning) in Spark.
   * @param properties
   *   A map of properties to configure the data source. Must include either "path" for local file
   *   access or "service" for HTTP access.
   * @return
   *   An instance of `StacTable`, wrapping the parsed STAC catalog JSON data.
   * @throws IllegalArgumentException
   *   If neither "url" nor "service" are provided.
   */
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val opts = new CaseInsensitiveStringMap(properties)
    val sparkSession = SparkSession.active

    val optsMap: Map[String, String] = opts.asCaseSensitiveMap().asScala.toMap ++ Map(
      "sessionLocalTimeZone" -> SparkSession.active.sessionState.conf.sessionLocalTimeZone,
      "columnNameOfCorruptRecord" -> SparkSession.active.sessionState.conf.columnNameOfCorruptRecord,
      "defaultParallelism" -> SparkSession.active.sparkContext.defaultParallelism.toString,
      "maxPartitionItemFiles" -> SparkSession.active.conf
        .get("spark.sedona.stac.load.maxPartitionItemFiles", "0"),
      "numPartitions" -> sparkSession.conf
        .get("spark.sedona.stac.load.numPartitions", "-1"),
      "itemsLimitMax" -> opts
        .asCaseSensitiveMap()
        .asScala
        .toMap
        .getOrElse(
          "itemsLimitMax",
          sparkSession.conf.get("spark.sedona.stac.load.itemsLimitMax", "-1")))
    val stacCollectionJsonString = StacUtils.loadStacCollectionToJson(optsMap)
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(opts.asScala.toMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    new StacTable(stacCollectionJson = stacCollectionJsonString, opts = optsMap, broadcastedConf)
  }
}
