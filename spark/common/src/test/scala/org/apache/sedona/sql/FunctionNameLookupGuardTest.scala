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

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files
import scala.collection.JavaConverters._

/**
 * Stops new by-name function lookups from entering main code unreviewed. Once a function prefix
 * is configured, a Sedona function reached through a plain string such as expr("ST_Buffer(...)")
 * no longer resolves. Every main source file that looks functions up by name must be listed here
 * with the reason it is safe.
 */
class FunctionNameLookupGuardTest extends AnyFunSuite {

  private val lookupPattern =
    """\b(expr|selectExpr|call_udf|call_function|callUDF)\s*\(|\.sql\s*\(|Column\.fn\s*\(""".r

  // Paths are relative to the spark/ directory and use forward slashes.
  private val allowed: Map[String, String] = Map(
    "common/src/main/scala-spark-4/org/apache/spark/sql/sedona_sql/DataFrameShims.scala" ->
      "resolves through SedonaFunctionNames",
    "common/src/main/scala/org/apache/spark/sql/sedona_sql/expressions/st_aggregates.scala" ->
      "resolves through SedonaFunctionNames",
    "common/src/main/scala/org/apache/sedona/stats/SpatialRowHash.scala" ->
      "ST_AsBinary / ST_SRID on native types are Spark's builtins under any prefix",
    "common/src/main/scala/org/apache/sedona/stats/autocorrelation/Moran.scala" ->
      "selectExpr only sums and aggregates Spark builtins over its own intermediate columns",
    "common/src/main/scala/org/apache/sedona/stats/outlierDetection/LocalOutlierFactor.scala" ->
      "expr is only the plain join condition n_id = b_id; the ST_KNN condition is built through st_predicates",
    "common/src/main/scala/org/apache/sedona/viz/sql/operator/VizPartitioner.scala" ->
      "expr wraps the secondarypid partition column name, not a function call")

  // Tests run with user.dir set to spark/common, so its parent holds the sibling modules.
  private def sparkDir: File =
    new File(System.getProperty("user.dir")).getAbsoluteFile.getParentFile

  // Every directory directly under spark/ that has a src/main, in a stable order.
  private def mainDirs: Seq[File] = {
    val modules = Option(sparkDir.listFiles()).getOrElse(Array.empty[File])
    modules
      .filter(_.isDirectory)
      .sortBy(_.getName)
      .map(new File(_, "src/main"))
      .filter(_.isDirectory)
      .toSeq
  }

  private def isCommentLine(line: String): Boolean = {
    val t = line.trim
    t.startsWith("//") || t.startsWith("*") || t.startsWith("/*")
  }

  private def looksUpByName(text: String): Boolean =
    text.split("\\r?\\n").exists(l => !isCommentLine(l) && lookupPattern.findFirstIn(l).isDefined)

  private def matchingFiles(dirs: Seq[File]): Set[String] = {
    val base = sparkDir.toPath
    dirs.flatMap { dir =>
      Files
        .walk(dir.toPath)
        .iterator()
        .asScala
        .filter(p => Files.isRegularFile(p))
        .filter(p => p.toString.endsWith(".scala") || p.toString.endsWith(".java"))
        .filter(p => looksUpByName(new String(Files.readAllBytes(p), "UTF-8")))
        .map(p => base.relativize(p).toString.replace(File.separatorChar, '/'))
        .toSeq
    }.toSet
  }

  test("main code looks up functions by name only in reviewed files") {
    val dirs = mainDirs
    val where =
      s"user.dir=${System.getProperty("user.dir")}, resolved spark directory=$sparkDir"
    assert(
      sparkDir.isDirectory && sparkDir.getName == "spark",
      s"The resolved directory is not the spark directory; run the test with user.dir set " +
        s"to the spark/common module ($where).")
    assert(
      dirs.contains(new File(sparkDir, "common/src/main")),
      s"No common/src/main found under the spark directory; run the test with user.dir " +
        s"set to the spark/common module ($where; walked: ${dirs.mkString(", ")}).")
    val found = matchingFiles(dirs)
    val unexpected = (found -- allowed.keySet).toSeq.sorted
    val stale = (allowed.keySet -- found).toSeq.sorted
    val message =
      "Unexpected files that look functions up by name:\n" +
        unexpected.map("  " + _).mkString("\n") +
        "\nStale allow-list entries that no longer match:\n" +
        stale.map("  " + _).mkString("\n") +
        "\nBuild the expression through st_functions / st_constructors / st_predicates, " +
        "or resolve the name with SedonaFunctionNames.resolve, then add the file here with a reason."
    assert(unexpected.isEmpty && stale.isEmpty, message)
  }
}
