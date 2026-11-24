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
import org.apache.sedona.sql.UDF.{Catalog, NativeFunctionRegistrationTest, SedonaFunctionNames}
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.sedona.sql.utils.GeoToolsCoverageAvailability.{gridClassName, isGeoToolsAvailable}
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.sedona_sql.UDT.RasterUdtRegistratorWrapper
import org.apache.spark.sql.sedona_sql.expressions.{st_constructors, st_functions}
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

/**
 * Sets the function name prefix on a session and checks what Sedona does before a registrator has
 * fixed the prefix in the JVM, so each test resets it first. Like FunctionPrefixSuite, the set-up
 * removes the plain Sedona names that earlier suites left behind, and the tear-down puts the JVM
 * back as it was.
 */
class FunctionPrefixBeforeRegistrationSuite extends AnyFunSpec with BeforeAndAfterAll {
  val P = "sedona_"

  private var fixture: NativeFunctionRegistrationTest.SessionFixture = _
  private def spark: SparkSession = fixture.spark

  private def isSedonaClass(c: String): Boolean =
    c.startsWith("org.apache.sedona.") || c.startsWith("org.apache.spark.sql.sedona_sql.")

  // The identifier forms the three registrators use for a plain name.
  private def identifier(name: String, namespace: String): FunctionIdentifier =
    if (SpatialTypeSupport.usesNativeTypes) {
      FunctionIdentifier(name, Some(namespace), Some("system"))
    } else {
      FunctionIdentifier(name)
    }

  // A platform function that owns a plain name.
  private def registerPlatformMarker(
      id: FunctionIdentifier,
      session: SparkSession = spark): Unit =
    session.sessionState.functionRegistry.registerFunction(
      id,
      new ExpressionInfo("org.example.PlatformMarker", null, id.funcName),
      (_: Seq[Expression]) => Literal(42))

  private def markerClass(id: FunctionIdentifier, session: SparkSession = spark): Option[String] =
    session.sessionState.functionRegistry.lookupFunction(id).map(_.getClassName)

  private def withActiveSession(session: SparkSession)(body: => Unit): Unit = {
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(session)
    try body
    finally {
      previous match {
        case Some(s) => SparkSession.setActiveSession(s)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

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
    // And from the JVM-wide builtin registry itself. fixture.close() puts the snapshot back.
    FunctionRegistry.builtin.listFunction().foreach { id =>
      if (FunctionRegistry.builtin.lookupFunction(id).exists(i => isSedonaClass(i.getClassName)))
        FunctionRegistry.builtin.dropFunction(id)
    }
    spark.conf.set(SedonaFunctionNames.PrefixConfigKey, P)
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

  describe("Sedona functions with a name prefix, before any registrator runs") {
    it("drops only prefixed names") {
      val markers = Seq(
        (() => Catalog.dropAll(spark), identifier("ST_Buffer", "builtin")),
        (() => SedonaVizRegistrator.dropAll(spark), identifier("ST_TileName", "builtin"))) ++
        (if (isGeoToolsAvailable)
           Seq((() => RasterRegistrator.dropAll(spark), identifier("RS_Stack_Aggr", "session")))
         else Seq.empty)
      // The active session has no prefix, so only the session handed to dropAll can supply it.
      withActiveSession(spark.newSession()) {
        markers.foreach { case (dropAll, id) =>
          SedonaFunctionNames.resetForTesting()
          registerPlatformMarker(id)
          dropAll()
          assert(markerClass(id).contains("org.example.PlatformMarker"), id.funcName)
          // dropAll fixes nothing, and the active session asks for no prefix.
          assert(SedonaFunctionNames.effectivePrefix == "", id.funcName)
        }
      }
    }

    it("lets the first registration fix the prefix after a drop") {
      val drops: Seq[SparkSession => Unit] = Seq(
        s => Catalog.dropAll(s),
        s => RasterRegistrator.dropAll(s),
        s => SedonaVizRegistrator.dropAll(s))
      drops.zipWithIndex.foreach { case (dropAll, i) =>
        SedonaFunctionNames.resetForTesting()
        // A session created from the context, so its conf has no prefix yet.
        val session = spark.newSession()
        withActiveSession(session) {
          dropAll(session)
          session.conf.set(SedonaFunctionNames.PrefixConfigKey, P)
          SedonaContext.create(session)
          assert(SedonaFunctionNames.effectivePrefix == P, s"drop $i")
          assert(
            session
              .sql(s"SELECT ${P}ST_AsText(${P}ST_Point(1.0, 2.0))")
              .head()
              .getString(0) == "POINT (1 2)",
            s"drop $i")
        }
      }
    }

    it("rejects a prefix that turns one Sedona name into another") {
      // With ST_, GeometryType would be registered as ST_GeometryType, a plain name.
      val calls: Seq[(String, SparkSession => Unit)] = Seq(
        "Catalog.registerAll" -> (s => Catalog.registerAll(s)),
        "RasterRegistrator.registerAll" -> (s => RasterRegistrator.registerAll(s)),
        "SedonaVizRegistrator.registerAll" -> (s => SedonaVizRegistrator.registerAll(s)),
        "Catalog.dropAll" -> (s => Catalog.dropAll(s)),
        "RasterRegistrator.dropAll" -> (s => RasterRegistrator.dropAll(s)),
        "SedonaVizRegistrator.dropAll" -> (s => SedonaVizRegistrator.dropAll(s)))
      val id = identifier("ST_GeometryType", "builtin")
      calls.foreach { case (call, run) =>
        SedonaFunctionNames.resetForTesting()
        val session = spark.newSession()
        session.conf.set(SedonaFunctionNames.PrefixConfigKey, "ST_")
        registerPlatformMarker(id, session)
        val e = intercept[IllegalArgumentException](run(session))
        Seq(SedonaFunctionNames.PrefixConfigKey, "'ST_'", " GeometryType ", "ST_GeometryType")
          .foreach(text => assert(e.getMessage.contains(text), s"$call: ${e.getMessage}"))
        assert(markerClass(id, session).contains("org.example.PlatformMarker"), call)
        // Nothing was fixed, so a later request still fixes its prefix.
        SedonaFunctionNames.initialize("sedona_")
        assert(SedonaFunctionNames.effectivePrefix == "sedona_", call)
      }
    }

    it("rejects a malformed prefix before dropping anything") {
      val drops: Seq[(SparkSession => Unit, FunctionIdentifier)] = Seq(
        ((s: SparkSession) => Catalog.dropAll(s), identifier("ST_Buffer", "builtin")),
        (
          (s: SparkSession) => SedonaVizRegistrator.dropAll(s),
          identifier("ST_TileName", "builtin"))) ++
        (if (isGeoToolsAvailable)
           Seq(
             (
               (s: SparkSession) => RasterRegistrator.dropAll(s),
               identifier("RS_Stack_Aggr", "session")))
         else Seq.empty)
      drops.foreach { case (dropAll, id) =>
        SedonaFunctionNames.resetForTesting()
        val session = spark.newSession()
        session.conf.set(SedonaFunctionNames.PrefixConfigKey, "bad-prefix")
        registerPlatformMarker(id, session)
        val e = intercept[IllegalArgumentException](dropAll(session))
        assert(e.getMessage.contains("'bad-prefix'"), id.funcName)
        // Dropping under no prefix would have removed the platform's function.
        assert(markerClass(id, session).contains("org.example.PlatformMarker"), id.funcName)
      }
    }

    it("registers the viz functions under the application's prefix too") {
      SedonaFunctionNames.resetForTesting()
      SedonaFunctionNames.useApplicationConf(spark.sparkContext, P)
      val client = spark.newSession()
      client.conf.set(SedonaFunctionNames.PrefixConfigKey, "other_")
      withActiveSession(client) {
        SedonaVizRegistrator.registerAll(client)
        assert(SedonaFunctionNames.effectivePrefix == P)
        assert(client.catalog.functionExists(P + "ST_TileName"))
        assert(!client.catalog.functionExists("other_ST_TileName"))
        SedonaVizRegistrator.dropAll(client)
      }
    }

    it("registers under the application's prefix for a session that asks for another") {
      SedonaFunctionNames.resetForTesting()
      SedonaFunctionNames.useApplicationConf(spark.sparkContext, P)
      // As the first session of a Spark Connect client that set the key before its first query.
      val client = spark.newSession()
      client.conf.set(SedonaFunctionNames.PrefixConfigKey, "other_")
      withActiveSession(client) {
        SedonaContext.create(client)
        assert(SedonaFunctionNames.effectivePrefix == P)
        assert(
          client
            .sql(s"SELECT ${P}ST_AsText(${P}ST_Point(1.0, 2.0))")
            .head()
            .getString(0) == "POINT (1 2)")
        assert(!client.catalog.functionExists("other_ST_Point"))
      }
    }

    it("aliases the Spark 4.2 names to Spark's own functions, not to a session's") {
      assume(
        SpatialTypeSupport.usesNativeTypes,
        "Spark 4.2 native spatial types are not available")
      SedonaFunctionNames.resetForTesting()
      val session = spark.newSession()
      session.conf.set(SedonaFunctionNames.PrefixConfigKey, P)
      // The session has its own ST_SRID before Sedona registers.
      registerPlatformMarker(identifier("ST_SRID", "builtin"), session)
      withActiveSession(session)(Catalog.registerAll(session))
      // The alias went into the JVM-wide builtin registry, so a fresh session shows it.
      val fresh = spark.newSession()
      val alias = fresh.sessionState.functionRegistry
        .lookupFunction(identifier(P + "ST_SRID", "builtin"))
        .map(_.getClassName)
      assert(
        alias.exists(_.startsWith("org.apache.spark.sql.catalyst.expressions.st.")),
        alias.toString)
      val srid = fresh
        .sql(s"SELECT ${P}ST_SRID(${P}ST_SetSRID(" +
          s"${P}ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'), 4326))")
        .head()
        .getInt(0)
      assert(srid == 4326)
    }

    it("resolves a DataFrame column built before SedonaContext.create") {
      SedonaFunctionNames.resetForTesting()
      withActiveSession(spark) {
        // On Spark 4.x the wrapper looks the function up by the name it resolves now.
        val point = st_constructors.ST_Point(lit(1.0), lit(2.0))
        SedonaContext.create(spark)
        val wkt = spark.range(1).select(st_functions.ST_AsText(point)).head().getString(0)
        assert(wkt == "POINT (1 2)")
      }
    }
  }
}
