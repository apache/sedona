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
package org.apache.sedona.sql

import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.sql.UDF.{Catalog, NativeFunctionRegistrationTest, PlainFunctionNames, RasterUdafCatalog, SedonaFunctionNames}
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.sedona.sql.utils.GeoToolsCoverageAvailability.{gridClassName, isGeoToolsAvailable}
import org.apache.sedona.stats.outlierDetection.LocalOutlierFactor
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.sedona_sql.UDT.RasterUdtRegistratorWrapper
import org.apache.spark.sql.sedona_sql.expressions.{st_aggregates, st_constructors, st_functions}
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.nio.file.Files
import java.util.Locale

/**
 * Registers Sedona with the function name prefix set. The prefix and FunctionRegistry.builtin are
 * JVM-wide, so the set-up removes the plain Sedona names that earlier suites left behind, and the
 * tear-down puts the JVM back as it was.
 */
class FunctionPrefixSuite extends AnyFunSpec with BeforeAndAfterAll {
  val P = "sedona_"

  private var fixture: NativeFunctionRegistrationTest.SessionFixture = _
  private def spark: SparkSession = fixture.spark
  private var prefixAfterViz: String = _

  private val nativeFive =
    Seq("ST_AsBinary", "ST_GeomFromWKB", "ST_GeogFromWKB", "ST_SRID", "ST_SetSRID")
  // Lazy: building the aggregators needs Spark to be set up.
  private lazy val scalarNames = Catalog.expressions.map(_._1.funcName)
  private lazy val aggregateNames = Catalog.aggregateExpressions.map(_.getClass.getSimpleName) ++
    Seq("ST_Envelope_Agg", "ST_Intersection_Agg", "ST_Union_Agg")
  private lazy val rasterNames =
    Seq(RasterUdafCatalog.rasterAggregateExpression.getClass.getSimpleName) // RS_Stack_Aggr
  private val vizNames =
    Seq("ST_Pixelize", "ST_TileName", "ST_Colorize", "ST_EncodeImage", "ST_Render")
  private lazy val allNames = scalarNames ++ aggregateNames ++ rasterNames ++ vizNames

  private def isSedonaClass(c: String): Boolean =
    c.startsWith("org.apache.sedona.") || c.startsWith("org.apache.spark.sql.sedona_sql.")

  // The identifier form AbstractCatalog.registryIdentifier uses.
  private def registryId(name: String): FunctionIdentifier =
    if (SpatialTypeSupport.usesNativeTypes) {
      FunctionIdentifier(name, Some("builtin"), Some("system"))
    } else {
      FunctionIdentifier(name)
    }

  // A platform function that owns a plain name before Sedona starts.
  private def registerPlatformMarker(): Unit =
    spark.sessionState.functionRegistry.registerFunction(
      registryId("ST_Buffer"),
      new ExpressionInfo("org.example.PlatformMarker", null, "ST_Buffer"),
      (_: Seq[Expression]) => Literal(42))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    fixture = new NativeFunctionRegistrationTest.SessionFixture(
      SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession))
    UdtRegistrator.registerAll()
    // RasterRegistrator.dropAll builds the raster aggregate, whose encoder needs the raster UDT.
    if (isGeoToolsAvailable) RasterUdtRegistratorWrapper.registerAll(gridClassName)
    // Remove the plain Sedona names this session inherited from the builtin registry.
    Catalog.dropAll(spark)
    RasterRegistrator.dropAll(spark)
    SedonaVizRegistrator.dropAll(spark)
    // And from the JVM-wide builtin registry itself, so sessions created later in this suite
    // start clean. fixture.close() puts the snapshot back.
    FunctionRegistry.builtin.listFunction().foreach { id =>
      if (FunctionRegistry.builtin.lookupFunction(id).exists(i => isSedonaClass(i.getClassName)))
        FunctionRegistry.builtin.dropFunction(id)
    }
    SedonaFunctionNames.resetForTesting()
    spark.conf.set(SedonaFunctionNames.PrefixConfigKey, P)
    registerPlatformMarker()
    // The viz registrator runs first on purpose: it must fix the prefix too.
    SedonaVizRegistrator.registerAll(spark)
    prefixAfterViz = SedonaFunctionNames.effectivePrefix
    SedonaContext.create(spark)
  }

  override protected def afterAll(): Unit = {
    try {
      Catalog.dropAll(spark)
      RasterRegistrator.dropAll(spark)
      SedonaVizRegistrator.dropAll(spark)
    } finally {
      SedonaFunctionNames.resetForTesting()
      fixture.close() // restores FunctionRegistry.builtin
      super.afterAll()
    }
  }

  describe("Sedona functions with a name prefix") {
    it("fixes the prefix when the viz registrator runs first") {
      assert(prefixAfterViz == P)
    }

    it("registers every function under the prefix") {
      val missing = allNames.filterNot(n => spark.catalog.functionExists(P + n))
      assert(missing.isEmpty, missing.mkString(", "))
    }

    it("checks the prefix against every name it registers") {
      assert(PlainFunctionNames.all.toSet == allNames.toSet)
    }

    it("registers core functions in the JVM-wide builtin registry under the prefix") {
      // Scalar and aggregate only; raster and viz are session-only.
      val missing = (scalarNames ++ aggregateNames).filterNot(n =>
        FunctionRegistry.builtin.functionExists(registryId(P + n)))
      assert(missing.isEmpty, missing.mkString(", "))
    }

    it("registers no Sedona class under a plain name") {
      val leaked = allNames.filter(n =>
        spark.sessionState.functionRegistry
          .lookupFunction(registryId(n))
          .exists(info => isSedonaClass(info.getClassName)))
      assert(leaked.isEmpty, leaked.mkString(", "))
    }

    it("leaves no Sedona class under a plain name in the builtin registry") {
      // The registry keeps names in lower case.
      val prefix = P.toLowerCase(Locale.ROOT)
      val leaked = FunctionRegistry.builtin.listFunction().filter { id =>
        !id.funcName.toLowerCase(Locale.ROOT).startsWith(prefix) &&
        FunctionRegistry.builtin.lookupFunction(id).exists(i => isSedonaClass(i.getClassName))
      }
      assert(leaked.isEmpty, leaked.mkString(", "))
    }

    it("leaves a platform function with a plain name alone") {
      assert(
        spark.sessionState.functionRegistry
          .lookupFunction(registryId("ST_Buffer"))
          .get
          .getClassName == "org.example.PlatformMarker")
      assert(spark.sql("SELECT ST_Buffer(1)").head().getInt(0) == 42)
    }

    it("runs prefixed SQL") {
      assert(
        spark
          .sql(s"SELECT ${P}ST_AsText(${P}ST_Point(1.0, 2.0))")
          .head()
          .getString(0) == "POINT (1 2)")
      assert(
        spark
          .sql(s"SELECT ${P}GeometryType(${P}ST_Point(1.0, 2.0))")
          .head()
          .getString(0) == "POINT")
    }

    it("resolves prefixed names in any letter case") {
      val area = spark
        .sql("SELECT SEDONA_st_area(sedona_ST_GEOMFROMTEXT('POLYGON((0 0,1 0,1 1,0 1,0 0))'))")
        .head()
        .getDouble(0)
      assert(area == 1.0)
    }

    it("describes the prefixed function") {
      val text = spark
        .sql(s"DESCRIBE FUNCTION ${P}ST_Buffer")
        .collect()
        .map(_.getString(0))
        .mkString("\n")
      assert(text.toLowerCase.contains("sedona_st_buffer"))
    }

    it("gives a second session the same prefixed names") {
      val second = spark.newSession()
      SedonaContext.create(second)
      assert(
        second
          .sql(s"SELECT ${P}ST_AsText(${P}ST_Point(1.0, 2.0))")
          .head()
          .getString(0) == "POINT (1 2)")
      assert(second.catalog.functionExists(P + "RS_Stack_Aggr"))
      assert(
        !second.catalog.functionExists("ST_Point") ||
          !isSedonaClass(
            second.sessionState.functionRegistry
              .lookupFunction(registryId("ST_Point"))
              .get
              .getClassName))
    }

    it("keeps the first prefix when another session asks for a different one") {
      val other = spark.newSession()
      other.conf.set(SedonaFunctionNames.PrefixConfigKey, "other_")
      SedonaContext.create(other) // must not throw
      assert(SedonaFunctionNames.effectivePrefix == P)
      assert(other.catalog.functionExists(P + "ST_Point"))
      // Session-only, so only the registration for this session can have put it there.
      assert(other.catalog.functionExists(P + "RS_Stack_Aggr"))
      assert(!other.catalog.functionExists("other_ST_Point"))
      // A session's own value stays as the session set it; it is ignored, not overwritten.
      assert(other.conf.get(SedonaFunctionNames.PrefixConfigKey) == "other_")
    }

    it("lists the marker function under the prefix, for a Spark Connect client to read") {
      // What a client runs as the first statement of its session, whatever its session asks for.
      val client = spark.newSession()
      client.conf.set(SedonaFunctionNames.PrefixConfigKey, "other_")
      SedonaContext.create(client)
      val listed = client
        .sql(s"SHOW FUNCTIONS LIKE '*${SedonaFunctionNames.MarkerFunctionName}'")
        .collect()
        .map(_.getString(0))
      assert(listed.length == 1, listed.mkString(", "))
      assert(SedonaFunctionNames.prefixOfListedNames(listed) == P)
    }

    it("drops and re-registers the prefixed names") {
      Catalog.dropAll(spark)
      assert(!spark.catalog.functionExists(P + "ST_Point"))
      assert(!spark.catalog.functionExists(P + "ST_Union_Agg"))
      Catalog.registerAll(spark)
      assert(spark.catalog.functionExists(P + "ST_Point"))
      assert(spark.catalog.functionExists(P + "ST_Union_Agg"))
    }

    it("aliases the five Spark 4.2 names to Spark's builtins") {
      assume(
        SpatialTypeSupport.usesNativeTypes,
        "Spark 4.2 native spatial types are not available")
      val registry = spark.sessionState.functionRegistry
      nativeFive.foreach { n =>
        assert(
          registry.lookupFunction(registryId(P + n)).get.getClassName ==
            registry.lookupFunction(registryId(n)).get.getClassName,
          n)
      }
      val srid = spark
        .sql(s"SELECT ${P}ST_SRID(${P}ST_SetSRID(" +
          s"${P}ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'), 4326))")
        .head()
        .getInt(0)
      assert(srid == 4326)
      Catalog.dropAll(spark)
      nativeFive.foreach { n =>
        assert(!spark.catalog.functionExists(P + n), n)
        assert(spark.catalog.functionExists(n), n) // Spark's own stay
      }
      Catalog.registerAll(spark)
    }

    it("runs scalar DataFrame wrappers") {
      val wkt = spark
        .range(1)
        .select(st_functions.ST_AsText(st_constructors.ST_Point(lit(1.0), lit(2.0))))
        .head()
        .getString(0)
      assert(wkt == "POINT (1 2)")
    }

    it("runs aggregate DataFrame wrappers") {
      val points =
        spark.sql(s"SELECT ${P}ST_Point(x, x) AS geom FROM VALUES (0.0), (1.0) AS t(x)")
      val row = points
        .select(
          st_functions.ST_NumGeometries(st_aggregates.ST_Collect_Agg(col("geom"))),
          st_functions.ST_AsText(st_aggregates.ST_Envelope_Aggr(col("geom"))),
          st_functions.ST_NumGeometries(st_aggregates.ST_Union_Aggr(col("geom"))),
          st_aggregates.ST_Extent(col("geom")),
          st_aggregates.ST_3DExtent(col("geom")))
        .head()
      assert(row.getInt(0) == 2)
      assert(row.getString(1) == "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
      assert(row.getInt(2) == 2)
      assert(row.get(3).toString == "BOX(0.0 0.0, 1.0 1.0)")
      assert(row.get(4).toString == "BOX3D(0.0 0.0 0.0, 1.0 1.0 0.0)")

      // Two squares that overlap in the unit square (1 1, 2 2).
      val squares = spark.sql(s"SELECT ${P}ST_GeomFromText(wkt) AS geom FROM VALUES " +
        "('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'), ('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))') AS t(wkt)")
      val intersection = squares
        .select(st_functions.ST_AsText(st_aggregates.ST_Intersection_Aggr(col("geom"))))
        .head()
        .getString(0)
      assert(intersection == "POLYGON ((1 2, 2 2, 2 1, 1 1, 1 2))")
    }

    it("runs LocalOutlierFactor") {
      // LocalOutlierFactor checkpoints, and a session this suite creates has no checkpoint
      // directory.
      if (spark.sparkContext.getCheckpointDir.isEmpty) {
        spark.sparkContext.setCheckpointDir(Files.createTempDirectory("checkpoints").toString)
      }
      val points = spark.sql(
        s"SELECT ${P}ST_Point(x, y) AS geometry FROM VALUES (0.0,0.0),(0.0,1.0),(1.0,0.0),(1.0,1.0),(9.0,9.0) AS t(x, y)")
      val result = LocalOutlierFactor.localOutlierFactor(points, 2)
      assert(result.count() == 5)
      assert(result.columns.contains("lof"))
    }

    it("still plans a prefixed predicate as a Sedona spatial join") {
      spark
        .sql(s"SELECT ${P}ST_Point(x, x) AS geom FROM VALUES (0.0), (1.0) AS t(x)")
        .createOrReplaceTempView("pa")
      // ${P}ST_Buffer is Sedona's; the platform marker sits on plain ST_Buffer only.
      spark
        .sql(
          s"SELECT ${P}ST_Buffer(${P}ST_Point(x, x), 0.5) AS geom FROM VALUES (0.0), (5.0) AS t(x)")
        .createOrReplaceTempView("pb")
      val joined = spark.sql(s"SELECT * FROM pa JOIN pb ON ${P}ST_Intersects(pa.geom, pb.geom)")
      val sedonaJoins = joined.queryExecution.sparkPlan.collect {
        case p
            if p.getClass.getName.startsWith("org.apache.spark.sql.sedona_sql.strategy.join") =>
          p
      }
      assert(sedonaJoins.nonEmpty)
      assert(joined.count() == 1)
    }
  }
}
