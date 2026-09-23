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
package org.apache.sedona.sql.UDF

import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.sedona_sql.expressions.{st_aggregates, st_constructors}
import org.apache.spark.sql.sedona_sql.expressions.st_functions
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

class NativeFunctionRegistrationTest extends AnyFunSpec with BeforeAndAfterAll {
  private val nativeFunctionNames =
    Seq("ST_AsBinary", "ST_GeomFromWKB", "ST_GeogFromWKB", "ST_SRID", "ST_SetSRID")
  private val version = SPARK_VERSION.split("\\.").take(2).map(_.toInt)
  private val usesNativeTypes = version(0) > 4 || (version(0) == 4 && version(1) >= 2)
  private val pointWkb = "0101000000000000000000F03F0000000000000040"
  private lazy val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("NativeFunctionRegistrationTest")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.geospatial.enabled", "true")
    .getOrCreate()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    UdtRegistrator.registerAll()
  }

  override protected def afterAll(): Unit = {
    try spark.stop()
    finally super.afterAll()
  }

  describe("Sedona function ownership") {
    it("preserves Spark 4.2 native functions across repeated registration and dropping") {
      assume(usesNativeTypes, "Spark 4.2 native spatial types are not available")
      val registry = spark.sessionState.functionRegistry
      val identifiers =
        nativeFunctionNames.map(name => FunctionIdentifier(name, Some("builtin"), Some("system")))
      val originalSession = identifiers.map(id =>
        (id, registry.lookupFunction(id).get, registry.lookupFunctionBuilder(id).get))
      val originalBuiltin = identifiers.map(id =>
        (
          id,
          FunctionRegistry.builtin.lookupFunction(id).get,
          FunctionRegistry.builtin.lookupFunctionBuilder(id).get))
      try {
        Catalog.registerAll(spark)
        Catalog.registerAll(spark)
        originalSession.foreach { case (id, info, _) =>
          assert(registry.lookupFunction(id).get.getClassName == info.getClassName, id.funcName)
        }
        originalBuiltin.foreach { case (id, info, _) =>
          assert(
            FunctionRegistry.builtin.lookupFunction(id).get.getClassName == info.getClassName,
            id.funcName)
        }
        Catalog.dropAll(spark)
        val row = spark
          .sql(s"""SELECT
          hex(ST_AsBinary(ST_GeomFromWKB(X'$pointWkb'))),
          ST_SRID(ST_GeogFromWKB(X'$pointWkb')),
          ST_SRID(ST_SetSRID(ST_GeomFromWKB(X'$pointWkb'), 4326))""")
          .head()
        assert(row.getString(0) == pointWkb)
        assert(row.getInt(1) == 4326)
        assert(row.getInt(2) == 4326)
      } finally {
        originalSession.foreach { case (id, info, builder) =>
          registry.registerFunction(id, info, builder)
        }
        originalBuiltin.foreach { case (id, info, builder) =>
          FunctionRegistry.builtin.registerFunction(id, info, builder)
        }
      }
    }

    it("uses native geography constructor arity and SRID defaults in Scala wrappers") {
      assume(usesNativeTypes, "Spark 4.2 native spatial types are not available")
      val frame = spark.sql(s"SELECT X'$pointWkb' AS wkb")
      val row = frame
        .select(
          st_functions.ST_SRID(st_constructors.ST_GeogFromWKB(col("wkb"))),
          st_functions.ST_SRID(st_constructors.ST_GeogFromWKB("wkb")),
          st_functions.ST_SRID(st_constructors.ST_GeogFromWKB(col("wkb"), lit(4269))),
          st_functions.ST_SRID(st_constructors.ST_GeogFromWKB("wkb", 4269)))
        .head()
      assert(row.toSeq == Seq(4326, 4326, 4269, 4269))
    }

    it("routes Scala aggregate wrappers through the native spatial adapter") {
      assume(usesNativeTypes, "Spark 4.2 native spatial types are not available")
      Catalog.registerAll(spark)
      val frame = spark.sql(s"SELECT ST_GeomFromWKB(X'$pointWkb', 4326) AS geom")
      Seq(
        st_aggregates.ST_Envelope_Aggr(col("geom")),
        st_aggregates.ST_Envelope_Agg("geom"),
        st_aggregates.ST_Intersection_Aggr("geom"),
        st_aggregates.ST_Intersection_Agg(col("geom")),
        st_aggregates.ST_Union_Aggr(col("geom")),
        st_aggregates.ST_Union_Agg("geom")).foreach { aggregate =>
        val result = frame.select(aggregate.as("geom"))
        assert(
          result.schema.head.dataType.getClass.getName ==
            "org.apache.spark.sql.types.GeometryType")
        assert(
          result
            .select(st_functions.ST_AsBinary(col("geom")))
            .head()
            .getAs[Array[Byte]](0)
            .nonEmpty)
      }
    }

    it("preserves DISTINCT on native spatial typed aggregates") {
      assume(usesNativeTypes, "Spark 4.2 native spatial types are not available")
      Catalog.registerAll(spark)
      val row = spark
        .sql("""
        SELECT ST_XMax(ST_Envelope_Aggr(DISTINCT geom)),
          ST_NumGeometries(ST_Collect_Agg(DISTINCT geom))
        FROM (SELECT ST_Point(cast(id % 3 AS double), 2D) AS geom FROM range(6))
      """)
        .head()
      assert(row.getDouble(0) == 2d)
      assert(row.getInt(1) == 3)
    }

    it("preserves FILTER on native spatial typed aggregates") {
      assume(usesNativeTypes, "Spark 4.2 native spatial types are not available")
      Catalog.registerAll(spark)
      val row = spark
        .sql("""
        SELECT ST_XMax(ST_Envelope_Aggr(geom) FILTER (WHERE id < 2))
        FROM (SELECT id, ST_Point(cast(id AS double), 2D) AS geom FROM range(4))
      """)
        .head()
      assert(row.getDouble(0) == 1d)
    }

    it("preserves window frames on native spatial typed aggregates") {
      assume(usesNativeTypes, "Spark 4.2 native spatial types are not available")
      Catalog.registerAll(spark)
      val rows = spark
        .sql("""
        SELECT id, ST_XMax(ST_Envelope_Aggr(geom) OVER (
          ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS xmax
        FROM (SELECT id, ST_Point(cast(id AS double), 2D) AS geom FROM range(4))
        ORDER BY id
      """)
        .collect()
      assert(rows.map(_.getDouble(1)).toSeq == Seq(0d, 1d, 2d, 3d))
    }

    it("registers and drops visualization functions in Spark 4.2 namespaces") {
      assume(usesNativeTypes, "Spark 4.2 function namespaces are not available")
      org.apache.sedona.viz.sql.UDF.UdfRegistrator.registerAll(spark)
      val registry = spark.sessionState.functionRegistry
      val identifier = FunctionIdentifier("ST_TileName", Some("builtin"), Some("system"))
      assert(registry.functionExists(identifier))
      org.apache.sedona.viz.sql.UDF.UdfRegistrator.dropAll(spark)
      assert(!registry.functionExists(identifier))
    }

    it("registers and drops raster aggregates in the Spark 4.2 session namespace") {
      assume(usesNativeTypes, "Spark 4.2 function namespaces are not available")
      assume(
        org.apache.sedona.sql.utils.GeoToolsCoverageAvailability.isGeoToolsAvailable,
        "GeoTools is required for raster aggregate registration")
      org.apache.sedona.sql.RasterRegistrator.registerAll(spark)
      val registry = spark.sessionState.functionRegistry
      val identifier = FunctionIdentifier("RS_Union_Aggr", Some("session"), Some("system"))
      assert(registry.functionExists(identifier))
      org.apache.sedona.sql.RasterRegistrator.dropAll(spark)
      assert(!registry.functionExists(identifier))
    }

    it("retains Sedona ownership and geography defaults before Spark 4.2") {
      assume(!usesNativeTypes, "Legacy function registration applies before Spark 4.2")
      Catalog.registerAll(spark)
      try {
        nativeFunctionNames.foreach { name =>
          assert(
            spark.sessionState.functionRegistry
              .lookupFunction(FunctionIdentifier(name))
              .get
              .getClassName
              .startsWith("org.apache.spark.sql.sedona_sql."),
            name)
        }
        val frame = spark.sql(s"SELECT X'$pointWkb' AS wkb")
        val row = frame
          .select(
            st_constructors.ST_GeogFromWKB(col("wkb")),
            st_constructors.ST_GeogFromWKB("wkb", 4326))
          .head()
        assert(row.getAs[org.apache.sedona.common.S2Geography.Geography](0).getSRID == 0)
        assert(row.getAs[org.apache.sedona.common.S2Geography.Geography](1).getSRID == 4326)
      } finally {
        Catalog.dropAll(spark)
      }
      nativeFunctionNames.foreach { name =>
        assert(
          !spark.sessionState.functionRegistry.functionExists(FunctionIdentifier(name)),
          name)
      }
    }
  }
}
