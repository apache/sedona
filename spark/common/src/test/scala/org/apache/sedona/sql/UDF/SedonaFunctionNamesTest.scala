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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funspec.AnyFunSpec

class SedonaFunctionNamesTest extends AnyFunSpec with BeforeAndAfterAll with BeforeAndAfterEach {

  override def beforeEach(): Unit = SedonaFunctionNames.resetForTesting()
  override def afterEach(): Unit = SedonaFunctionNames.resetForTesting()

  // The session the conf tests derive theirs from: one that is already running, else a local
  // one that afterAll stops.
  private var createdSession: Option[SparkSession] = None
  private lazy val baseSession: SparkSession =
    SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .filterNot(_.sparkContext.isStopped)
      .getOrElse {
        val session = SparkSession
          .builder()
          .master("local[1]")
          .appName("SedonaFunctionNamesTest")
          .config("spark.ui.enabled", "false")
          .getOrCreate()
        createdSession = Some(session)
        session
      }

  override def afterAll(): Unit =
    try createdSession.foreach(_.stop())
    finally super.afterAll()

  // Runs `body` with a new session, whose conf holds `conf`, as the active session.
  private def withActiveSession(conf: (String, String)*)(body: => Unit): Unit = {
    val session = baseSession.newSession()
    conf.foreach { case (key, value) => session.conf.set(key, value) }
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

  describe("SedonaFunctionNames") {
    it("resolves to the plain name before any prefix is fixed") {
      assert(SedonaFunctionNames.effectivePrefix == "")
      assert(SedonaFunctionNames.resolve("ST_Buffer") == "ST_Buffer")
    }
    it("prepends the prefix verbatim to every name") {
      SedonaFunctionNames.initialize("sedona_")
      assert(SedonaFunctionNames.resolve("ST_Buffer") == "sedona_ST_Buffer")
      assert(SedonaFunctionNames.resolve("GeometryType") == "sedona_GeometryType")
    }
    it("keeps the first prefix when a later one differs") {
      SedonaFunctionNames.initialize("sedona_")
      SedonaFunctionNames.initialize("other_")
      SedonaFunctionNames.initialize("")
      assert(SedonaFunctionNames.effectivePrefix == "sedona_")
    }
    it("keeps an empty first prefix") {
      SedonaFunctionNames.initialize("")
      SedonaFunctionNames.initialize("sedona_")
      assert(SedonaFunctionNames.effectivePrefix == "")
    }
    it("accepts identifier-shaped prefixes") {
      Seq("sedona_", "_s", "S1_", "a").foreach { p =>
        SedonaFunctionNames.resetForTesting()
        SedonaFunctionNames.initialize(p)
        assert(SedonaFunctionNames.effectivePrefix == p)
      }
    }
    it("rejects other values and does not fix them") {
      Seq("1sedona", "sedona-", "sedona.", " sedona_", "sedona_ ", "se dona", "sedona_;")
        .foreach { p =>
          val e = intercept[IllegalArgumentException](SedonaFunctionNames.initialize(p))
          assert(e.getMessage.contains("spark.sedona.function.prefix"))
          assert(e.getMessage.contains(p))
          assert(SedonaFunctionNames.effectivePrefix == "")
        }
      SedonaFunctionNames.initialize("sedona_")
      assert(SedonaFunctionNames.effectivePrefix == "sedona_")
    }
    it("rejects a prefix that turns one name into another, ignoring case, and does not fix it") {
      Seq("ST_", "st_").foreach { p =>
        val e = intercept[IllegalArgumentException](
          SedonaFunctionNames.initialize(p, Seq("ST_GeometryType", "GeometryType")))
        assert(e.getMessage.contains("spark.sedona.function.prefix"), p)
        assert(e.getMessage.contains(s"'$p'"), p)
        assert(e.getMessage.contains(s" GeometryType as ${p}GeometryType"), e.getMessage)
        assert(SedonaFunctionNames.effectivePrefix == "", p)
      }
      SedonaFunctionNames.initialize("sedona_")
      assert(SedonaFunctionNames.effectivePrefix == "sedona_")
    }
    it("accepts a prefix under which no Sedona name becomes another") {
      Seq("sedona_", "st_x_").foreach { p =>
        SedonaFunctionNames.resetForTesting()
        SedonaFunctionNames.initialize(p, PlainFunctionNames.all)
        assert(SedonaFunctionNames.effectivePrefix == p)
      }
    }
    it("checks no names once a prefix is fixed") {
      SedonaFunctionNames.initialize("sedona_")
      SedonaFunctionNames.initialize("ST_", Seq("ST_GeometryType", "GeometryType")) // ignored
      assert(SedonaFunctionNames.effectivePrefix == "sedona_")
    }
    it("reads the active session's prefix before one is fixed, without fixing it") {
      withActiveSession(SedonaFunctionNames.PrefixConfigKey -> "sedona_") {
        assert(SedonaFunctionNames.resolve("ST_Point") == "sedona_ST_Point")
        SedonaFunctionNames.initialize("other_")
        assert(SedonaFunctionNames.effectivePrefix == "other_")
        assert(SedonaFunctionNames.resolve("ST_Point") == "other_ST_Point")
      }
    }
    it("resolves to the plain name while the active session's prefix is invalid") {
      withActiveSession(SedonaFunctionNames.PrefixConfigKey -> "sedona-") {
        assert(SedonaFunctionNames.resolve("ST_Point") == "ST_Point")
      }
    }
    it("takes the prefix from the application's Spark config when that decides") {
      // As after SedonaSqlExtensions was loaded with sedona_ in the application's config.
      SedonaFunctionNames.useApplicationConf(
        new SparkConf(false).set(SedonaFunctionNames.PrefixConfigKey, "sedona_"))
      withActiveSession(SedonaFunctionNames.PrefixConfigKey -> "other_") {
        // Before a registrator runs, and when one runs for the session that asks for other_.
        assert(SedonaFunctionNames.resolve("ST_Point") == "sedona_ST_Point")
        SedonaFunctionNames.initialize(SparkSession.getActiveSession.get, Nil)
        assert(SedonaFunctionNames.effectivePrefix == "sedona_")
      }
    }
    it("takes no prefix when the application's Spark config has none and that decides") {
      SedonaFunctionNames.useApplicationConf(new SparkConf(false))
      withActiveSession(SedonaFunctionNames.PrefixConfigKey -> "other_") {
        assert(SedonaFunctionNames.resolve("ST_Point") == "ST_Point")
        SedonaFunctionNames.initialize(SparkSession.getActiveSession.get, Nil)
        assert(SedonaFunctionNames.effectivePrefix == "")
      }
    }
    it("rejects an invalid prefix of the application's config whatever the session asks") {
      SedonaFunctionNames.useApplicationConf(
        new SparkConf(false).set(SedonaFunctionNames.PrefixConfigKey, "bad-prefix"))
      withActiveSession(SedonaFunctionNames.PrefixConfigKey -> "sedona_") {
        val session = SparkSession.getActiveSession.get
        val e = intercept[IllegalArgumentException](SedonaFunctionNames.initialize(session, Nil))
        assert(e.getMessage.contains("'bad-prefix'"))
        intercept[IllegalArgumentException](SedonaFunctionNames.prefixToDrop(session, Nil))
        assert(SedonaFunctionNames.resolve("ST_Point") == "ST_Point")
      }
    }
    it("rejects an invalid prefix when dropping, as when registering") {
      withActiveSession(SedonaFunctionNames.PrefixConfigKey -> "bad-prefix") {
        val e = intercept[IllegalArgumentException](
          SedonaFunctionNames.prefixToDrop(SparkSession.getActiveSession.get, Nil))
        assert(e.getMessage.contains(SedonaFunctionNames.PrefixConfigKey))
        assert(e.getMessage.contains("'bad-prefix'"))
      }
    }
    it("reads the prefix from the names a server lists for the marker function") {
      val marker = SedonaFunctionNames.MarkerFunctionName
      assert(SedonaFunctionNames.prefixOfListedNames(Seq("sedona_" + marker)) == "sedona_")
      assert(SedonaFunctionNames.prefixOfListedNames(Seq(marker)) == "")
      // No Sedona on the server, or a Sedona from before the prefix.
      assert(SedonaFunctionNames.prefixOfListedNames(Nil) == "")
    }
    it("resolves to the plain name while the active session has no prefix") {
      withActiveSession() {
        assert(SedonaFunctionNames.resolve("ST_Point") == "ST_Point")
      }
    }
  }
}
