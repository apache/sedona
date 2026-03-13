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
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.io.geojson.GeoJSONUtils
import org.apache.spark.sql.sedona_sql.io.stac.StacUtils.{inferStacSchema, updatePropertiesPromotedSchema}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.util.concurrent.ConcurrentHashMap

/**
 * The `StacTable` class represents a table in the SpatioTemporal Asset Catalog (STAC) data
 * source.
 *
 * This class implements the `Table` and `SupportsRead` interfaces to integrate with Apache
 * Spark's data source API, providing support for reading data from STAC.
 *
 * @constructor
 *   Creates a new instance of the `StacTable` class.
 */
class StacTable(
    stacCollectionJson: String,
    opts: Map[String, String],
    broadcastConf: Broadcast[SerializableConfiguration])
    extends Table
    with SupportsRead {

  // Cache to store inferred schemas
  private val schemaCache = new ConcurrentHashMap[Map[String, String], StructType]()

  /**
   * Returns the name of the table.
   *
   * @return
   *   The name of the table as a string.
   */
  override def name(): String = "stac"

  /**
   * Defines the schema of the STAC table.
   *
   * @return
   *   The schema as a StructType.
   */
  override def schema(): StructType = {
    // Check if the schema is already cached
    val fullSchema = schemaCache.computeIfAbsent(opts, _ => inferStacSchema(opts))
    val updatedGeometrySchema = GeoJSONUtils.updateGeometrySchema(fullSchema, GeometryUDT())
    updatePropertiesPromotedSchema(updatedGeometrySchema)
  }

  /**
   * Indicates the capabilities supported by the STAC table, specifically batch read.
   *
   * @return
   *   A set of table capabilities.
   */
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  /**
   * Creates a new scan builder for reading data from the STAC table.
   *
   * @param options
   *   The configuration options for the scan.
   * @return
   *   A new instance of ScanBuilder.
   */
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new StacScanBuilder(stacCollectionJson, opts, broadcastConf)
}

object StacTable {

  /**
   * Defines the schema of the STAC table, which supports various fields including collection
   * information, asset details, geometries, and more. The schema is based on the STAC
   * specification version 1.1.0.
   */
  val SCHEMA_V1_1_0: StructType = StructType(
    Seq(
      StructField("stac_version", StringType, nullable = false),
      StructField("stac_extensions", ArrayType(StringType), nullable = true),
      StructField("type", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("bbox", ArrayType(DoubleType), nullable = true),
      StructField(
        "geometry",
        StructType(
          Seq(
            StructField("type", StringType, nullable = true),
            StructField("coordinates", ArrayType(ArrayType(DoubleType)), nullable = true))),
        nullable = true),
      StructField(
        "properties",
        StructType(Seq(
          StructField("title", StringType, nullable = true),
          StructField("description", StringType, nullable = true),
          StructField("datetime", TimestampType, nullable = true),
          StructField("start_datetime", TimestampType, nullable = true),
          StructField("end_datetime", TimestampType, nullable = true),
          StructField("created", TimestampType, nullable = true),
          StructField("updated", TimestampType, nullable = true),
          StructField("platform", StringType, nullable = true),
          StructField("instruments", ArrayType(StringType), nullable = true),
          StructField("constellation", StringType, nullable = true),
          StructField("mission", StringType, nullable = true),
          StructField("gsd", DoubleType, nullable = true))),
        nullable = false),
      StructField("collection", StringType, nullable = true),
      StructField(
        "links",
        ArrayType(StructType(Seq(
          StructField("rel", StringType, nullable = true),
          StructField("href", StringType, nullable = true),
          StructField("type", StringType, nullable = true),
          StructField("title", StringType, nullable = true)))),
        nullable = false),
      StructField(
        "assets",
        MapType(
          StringType,
          StructType(Seq(
            StructField("href", StringType, nullable = true),
            StructField("type", StringType, nullable = true),
            StructField("title", StringType, nullable = true),
            StructField("roles", ArrayType(StringType), nullable = true)))),
        nullable = false)))

  /**
   * Defines the schema of the STAC table, which supports various fields including collection
   * information, asset details, geometries, and more. The schema is based on the STAC
   * specification version 1.0.0.
   */
  val SCHEMA_V1_0_0: StructType = StructType(
    Seq(
      StructField("stac_version", StringType, nullable = false),
      StructField("stac_extensions", ArrayType(StringType), nullable = true),
      StructField("type", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("bbox", ArrayType(DoubleType), nullable = true),
      StructField(
        "geometry",
        StructType(
          Seq(
            StructField("type", StringType, nullable = true),
            StructField("coordinates", ArrayType(ArrayType(DoubleType)), nullable = true))),
        nullable = true),
      StructField(
        "properties",
        StructType(Seq(
          StructField("title", StringType, nullable = true),
          StructField("description", StringType, nullable = true),
          StructField("datetime", TimestampType, nullable = true),
          StructField("start_datetime", TimestampType, nullable = true),
          StructField("end_datetime", TimestampType, nullable = true),
          StructField("created", TimestampType, nullable = true),
          StructField("updated", TimestampType, nullable = true),
          StructField("platform", StringType, nullable = true),
          StructField("instruments", ArrayType(StringType), nullable = true),
          StructField("constellation", StringType, nullable = true),
          StructField("mission", StringType, nullable = true),
          StructField("gsd", DoubleType, nullable = true))),
        nullable = true),
      StructField("collection", StringType, nullable = true),
      StructField(
        "links",
        ArrayType(StructType(Seq(
          StructField("rel", StringType, nullable = true),
          StructField("href", StringType, nullable = true),
          StructField("type", StringType, nullable = true),
          StructField("title", StringType, nullable = true)))),
        nullable = true),
      StructField(
        "assets",
        MapType(
          StringType,
          StructType(Seq(
            StructField("href", StringType, nullable = true),
            StructField("type", StringType, nullable = true),
            StructField("title", StringType, nullable = true),
            StructField("roles", ArrayType(StringType), nullable = true)))),
        nullable = true)))

  val SCHEMA_GEOPARQUET: StructType = StructType(
    Seq(
      StructField("stac_version", StringType, nullable = false),
      StructField("stac_extensions", ArrayType(StringType), nullable = true),
      StructField("type", StringType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("bbox", ArrayType(DoubleType), nullable = true),
      StructField(
        "geometry",
        StructType(
          Seq(
            StructField("type", StringType, nullable = true),
            StructField("coordinates", ArrayType(ArrayType(DoubleType)), nullable = true))),
        nullable = true),
      StructField("datetime", TimestampType, nullable = true),
      StructField("collection", StringType, nullable = true),
      StructField(
        "links",
        ArrayType(StructType(Seq(
          StructField("rel", StringType, nullable = true),
          StructField("href", StringType, nullable = true),
          StructField("type", StringType, nullable = true),
          StructField("title", StringType, nullable = true)))),
        nullable = false)))

  def addAssetStruct(schema: StructType, name: String): StructType = {
    val assetStruct = StructType(
      Seq(
        StructField("href", StringType, nullable = true),
        StructField("roles", ArrayType(StringType), nullable = true),
        StructField("title", StringType, nullable = true),
        StructField("type", StringType, nullable = true)))

    val updatedFields = schema.fields.map {
      case StructField("assets", existingStruct: StructType, nullable, metadata) =>
        StructField(
          "assets",
          StructType(existingStruct.fields :+ StructField(name, assetStruct, nullable = true)),
          nullable,
          metadata)
      case other => other
    }

    if (!schema.fieldNames.contains("assets")) {
      StructType(
        updatedFields :+ StructField(
          "assets",
          StructType(Seq(StructField(name, assetStruct, nullable = true))),
          nullable = true))
    } else {
      StructType(updatedFields)
    }
  }

  def addAssetsStruct(schema: StructType, names: Array[String]): StructType = {
    names.foldLeft(schema) { (currentSchema, name) =>
      addAssetStruct(currentSchema, name)
    }
  }
}
