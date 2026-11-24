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
import org.apache.sedona.sql.UDF.SedonaFunctionNames
import org.apache.sedona.viz.sql.SedonaVizExtensions
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/**
 * A Spark application that loads one Sedona extension with the function name prefix chart_ on its
 * builder. It prints the prefix Sedona answers with before the first query of its first session,
 * and the names that session has Sedona's functions under after it.
 *
 * Arguments: how the extension is loaded ("withExtensions" or "conf"), the extension's class
 * name, the prefix the session asks for before its first query ("-" for none), and "create" if
 * the application calls SedonaContext.create before that query ("-" if not).
 */
object FunctionPrefixFreshApplication {
  val Prefix = "chart_"
  val ResultMarker = "SEDONA_FUNCTION_NAMES="
  val PrefixBeforeMarker = "SEDONA_PREFIX_BEFORE_FIRST_QUERY="

  private def isSedonaClass(c: String): Boolean =
    Seq(
      "org.apache.sedona.",
      "org.apache.spark.sql.sedona_sql.",
      "org.apache.spark.sql.sedona_viz.")
      .exists(c.startsWith)

  def main(args: Array[String]): Unit = {
    val Array(how, extensionClass, sessionAsks, create) = args
    // The extension comes first, as in an application that builds its session in one chain: at
    // this point the builder has no master, no application name and no prefix yet.
    val builder = how match {
      case "withExtensions" =>
        val extension = Class
          .forName(extensionClass)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[SparkSessionExtensions => Unit]
        SparkSession.builder().withExtensions(extension)
      case "conf" =>
        SparkSession.builder().config("spark.sql.extensions", extensionClass)
    }
    val spark = builder
      .master("local[1]")
      .appName("FunctionPrefixFreshApplication")
      .config("spark.ui.enabled", "false")
      .config(SedonaFunctionNames.PrefixConfigKey, Prefix)
      .getOrCreate()
    try {
      if (sessionAsks != "-") spark.conf.set(SedonaFunctionNames.PrefixConfigKey, sessionAsks)
      // What a DataFrame column built now would carry.
      val prefixBefore = SedonaFunctionNames.effectivePrefix
      // Spark has not built the session's check rules yet, so the extension has not registered.
      if (create == "create") SedonaContext.create(spark)
      // The first analysis of the session: Spark builds the check rules, and Sedona registers.
      spark.sql("SELECT 1").collect()
      val registry = spark.sessionState.functionRegistry
      val names = registry.listFunction().flatMap { id =>
        registry.lookupFunction(id).filter(info => isSedonaClass(info.getClassName)).map { _ =>
          id.funcName.toLowerCase(Locale.ROOT)
        }
      }
      // scalastyle:off println
      println(PrefixBeforeMarker + prefixBefore)
      println(ResultMarker + names.sorted.mkString(","))
      // scalastyle:on println
    } finally spark.stop()
  }
}

/**
 * Starts Spark applications of their own, each in a new JVM: the JVM of the tests has a running
 * SparkContext and a function registry that earlier suites wrote to, and what is tested here is
 * what an extension does before the application's SparkContext exists.
 */
class FunctionPrefixFreshApplicationSuite extends AnyFunSpec {
  private val prefix = FunctionPrefixFreshApplication.Prefix

  // The JVM options the tests run with that Spark needs on a recent JDK.
  private def inheritedJvmOptions: Seq[String] =
    ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.toSeq.filter { option =>
      option.startsWith("--add-") || option == "-XX:+IgnoreUnrecognizedVMOptions" ||
      option.startsWith("-Djdk.reflect.")
    }

  private def location(c: Class[_]): String =
    Paths.get(c.getProtectionDomain.getCodeSource.getLocation.toURI).toString

  /**
   * Runs FunctionPrefixFreshApplication in a new JVM and returns the lower-case names it printed.
   */
  private def sedonaFunctionNames(jvmOptions: Seq[String], arguments: String*): Seq[String] =
    runApplication(jvmOptions, arguments: _*)._2

  /**
   * Runs FunctionPrefixFreshApplication in a new JVM. Returns the prefix it printed, and the
   * lower-case function names.
   */
  private def runApplication(
      jvmOptions: Seq[String],
      arguments: String*): (String, Seq[String]) = {
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val command = Seq(java) ++ inheritedJvmOptions ++ jvmOptions ++
      Seq(FunctionPrefixFreshApplication.getClass.getName.stripSuffix("$")) ++ arguments
    val output = File.createTempFile("function-prefix-fresh-application", ".log")
    try {
      val process = new ProcessBuilder(command: _*)
      // In the environment, not on the command line, which has a length limit.
      val classPath = Seq(
        location(FunctionPrefixFreshApplication.getClass),
        location(classOf[SedonaVizExtensions]),
        System.getProperty("java.class.path")).distinct
      process.environment().put("CLASSPATH", classPath.mkString(File.pathSeparator))
      process.redirectErrorStream(true).redirectOutput(output)
      val running = process.start()
      val finished = running.waitFor(5, TimeUnit.MINUTES)
      if (!finished) running.destroyForcibly()
      val lines = Files.readAllLines(output.toPath, StandardCharsets.UTF_8).asScala.toSeq
      val log = lines.takeRight(60).mkString("\n")
      assert(finished, s"the application did not finish:\n$log")
      assert(running.exitValue() == 0, s"the application failed:\n$log")
      def printed(marker: String): String = {
        val line = lines.find(_.startsWith(marker))
        assert(line.isDefined, s"the application printed no $marker line:\n$log")
        line.get.stripPrefix(marker)
      }
      val names =
        printed(FunctionPrefixFreshApplication.ResultMarker).split(",").filter(_.nonEmpty)
      (printed(FunctionPrefixFreshApplication.PrefixBeforeMarker), names.toSeq)
    } finally output.delete()
  }

  private def assertOnlyPrefixed(names: Seq[String], expected: String): Unit = {
    assert(names.contains(prefix + expected), names.mkString(","))
    val others = names.filterNot(_.startsWith(prefix))
    assert(others.isEmpty, s"registered without the prefix: ${others.mkString(",")}")
  }

  describe("A Sedona extension in a new Spark application with a function name prefix") {
    it("registers only chart_ST_TileName when the builder installs SedonaVizExtensions") {
      val names =
        sedonaFunctionNames(Nil, "withExtensions", classOf[SedonaVizExtensions].getName, "-", "-")
      assertOnlyPrefixed(names, "st_tilename")
      assert(names.filter(_.endsWith("st_tilename")) == Seq(prefix + "st_tilename"))
    }

    it("takes the builder's prefix when the JVM has a master and an application name too") {
      // As under spark-submit. A SparkContext created before the builder's options apply would
      // start from these properties alone, without the prefix.
      val submitted = Seq("-Dspark.master=local[1]", "-Dspark.app.name=submitted")
      val names =
        sedonaFunctionNames(
          submitted,
          "withExtensions",
          classOf[SedonaVizExtensions].getName,
          "-",
          "-")
      assertOnlyPrefixed(names, "st_tilename")
      assert(names.filter(_.endsWith("st_tilename")) == Seq(prefix + "st_tilename"))
    }

    it("starts when the builder installs SedonaSqlExtensions") {
      // Nothing gives the JVM a master or an application name. Only the builder does, after it
      // has loaded the extension.
      val names =
        sedonaFunctionNames(Nil, "withExtensions", classOf[SedonaSqlExtensions].getName, "-", "-")
      assertOnlyPrefixed(names, "st_point")
    }

    it("takes the builder's prefix with SedonaSqlExtensions when the JVM has a master too") {
      // As under spark-submit: a SparkContext started when the extension is loaded would not
      // have the builder's prefix.
      val submitted = Seq("-Dspark.master=local[1]", "-Dspark.app.name=submitted")
      val names =
        sedonaFunctionNames(
          submitted,
          "withExtensions",
          classOf[SedonaSqlExtensions].getName,
          "-",
          "-")
      assertOnlyPrefixed(names, "st_point")
    }

    it("keeps the application's prefix when the first session asks for another") {
      Seq(
        classOf[SedonaVizExtensions].getName -> "st_tilename",
        classOf[SedonaSqlExtensions].getName -> "st_point").foreach {
        case (extension, expected) =>
          withClue(extension) {
            // The application names the extension in spark.sql.extensions, so its config
            // decides from the start, also for a column built before the first query.
            val (prefixBefore, names) = runApplication(Nil, "conf", extension, "other_", "-")
            assert(prefixBefore == prefix)
            assertOnlyPrefixed(names, expected)
          }
      }
    }

    it("keeps the application's prefix when the builder installs the extension too") {
      val (prefixBefore, names) =
        runApplication(Nil, "withExtensions", classOf[SedonaVizExtensions].getName, "other_", "-")
      assert(prefixBefore == prefix)
      assertOnlyPrefixed(names, "st_tilename")
    }

    it("keeps the application's prefix when SedonaContext.create runs before the first query") {
      // The builder installed the extension, the session asks for another prefix, and the
      // application registers by hand before Spark has called the extension for the session.
      // That registration is the first in the JVM, so it fixes the prefix.
      val (prefixBefore, names) =
        runApplication(
          Nil,
          "withExtensions",
          classOf[SedonaVizExtensions].getName,
          "other_",
          "create")
      assert(prefixBefore == prefix)
      assertOnlyPrefixed(names, "st_point")
      assertOnlyPrefixed(names, "st_tilename")
    }
  }
}
