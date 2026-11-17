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
package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * Helper for creating a [[PartitioningAwareFileIndex]] without going through the
 * [[org.apache.spark.sql.execution.streaming.FileStreamSink.hasMetadata]] check in
 * [[org.apache.spark.sql.execution.datasources.v2.FileTable.fileIndex]].
 *
 * <p>The streaming metadata check can produce spurious [[java.io.FileNotFoundException]] warnings
 * when reading from cloud storage (e.g., S3) because it attempts to stat the path as a directory.
 * For non-streaming, read-only file tables such as Shapefile and GeoPackage, this check is
 * unnecessary and can be safely bypassed.
 */
object SedonaFileIndexHelper {

  private val CheckAndGlobMethodName = "checkAndGlobPathIfNecessary"

  /**
   * Cached reflective reference to [[DataSource.checkAndGlobPathIfNecessary]].
   *
   * <p>We call this method via reflection to avoid binary incompatibility between OSS Apache
   * Spark and Databricks Runtime. On OSS Spark (3.5, 4.0, 4.1, etc.) this method has default
   * parameter values, but on Databricks Runtime (both Spark 3.5 and 4.0) the same method has all
   * required parameters with no defaults, and may also differ in parameter count.
   *
   * <p>Direct calls with named/default parameters cause the Scala compiler to generate synthetic
   * {@code $default$N()} accessor methods in the bytecode. When these accessors do not exist at
   * runtime (as is the case on Databricks), a {@link NoSuchMethodError} is thrown. Reflection
   * avoids this by resolving the method at runtime.
   *
   * <p>The overload is chosen by its parameter types, see [[resolveCheckAndGlobMethod]].
   */
  private lazy val checkAndGlobMethod: java.lang.reflect.Method =
    resolveCheckAndGlobMethod(
      DataSource.getClass.getMethods.toSeq.filter(_.getName == CheckAndGlobMethodName))

  private def isSeq(c: Class[_]): Boolean = classOf[scala.collection.Seq[_]].isAssignableFrom(c)

  private def isBoolean(c: Class[_]): Boolean =
    c == java.lang.Boolean.TYPE || c == classOf[java.lang.Boolean]

  private def isInt(c: Class[_]): Boolean =
    c == java.lang.Integer.TYPE || c == classOf[java.lang.Integer]

  // OSS Apache Spark: (paths, hadoopConf, checkEmptyGlobPath, checkFilesExist, numThreads,
  // enableGlobbing)
  private def isOssSignature(m: java.lang.reflect.Method): Boolean = {
    val t = m.getParameterTypes
    t.length == 6 && isSeq(t(0)) && classOf[Configuration].isAssignableFrom(t(1)) &&
    isBoolean(t(2)) && isBoolean(t(3)) && isInt(t(4)) && isBoolean(t(5))
  }

  // Databricks Runtime: (paths, hadoopConf, checkEmptyGlobPath, checkFilesExist, enableGlobbing)
  private def isDatabricksSignature(m: java.lang.reflect.Method): Boolean = {
    val t = m.getParameterTypes
    t.length == 5 && isSeq(t(0)) && classOf[Configuration].isAssignableFrom(t(1)) &&
    isBoolean(t(2)) && isBoolean(t(3)) && isBoolean(t(4))
  }

  /**
   * Pick the overload of `checkAndGlobPathIfNecessary` to call from the candidates found by
   * reflection.
   *
   * <p>`Class.getMethods` returns overloads in no particular order, and some Spark distributions
   * (e.g. Google Dataproc) declare additional overloads next to the standard one, so the choice
   * must be made on the parameter types and never on the order or on the first match by name.
   */
  private[datasources] def resolveCheckAndGlobMethod(
      candidates: Seq[java.lang.reflect.Method]): java.lang.reflect.Method = {
    candidates
      .find(isOssSignature)
      .orElse(candidates.find(isDatabricksSignature))
      .getOrElse {
        val found = candidates.map(_.toGenericString).sorted.mkString("[", ", ", "]")
        throw new NoSuchMethodException(
          s"No supported overload of DataSource.$CheckAndGlobMethodName found. Candidates: $found")
      }
  }

  private def checkAndGlobPathIfNecessary(
      paths: Seq[String],
      hadoopConf: Configuration,
      checkEmptyGlobPath: Boolean,
      checkFilesExist: Boolean,
      enableGlobbing: Boolean): Seq[Path] = {
    val method = checkAndGlobMethod
    val args: Array[AnyRef] = if (isOssSignature(method)) {
      Array(
        paths,
        hadoopConf,
        java.lang.Boolean.valueOf(checkEmptyGlobPath),
        java.lang.Boolean.valueOf(checkFilesExist),
        Integer.valueOf(40),
        java.lang.Boolean.valueOf(enableGlobbing))
    } else {
      Array(
        paths,
        hadoopConf,
        java.lang.Boolean.valueOf(checkEmptyGlobPath),
        java.lang.Boolean.valueOf(checkFilesExist),
        java.lang.Boolean.valueOf(enableGlobbing))
    }
    try {
      method.invoke(DataSource, args: _*).asInstanceOf[Seq[Path]]
    } catch {
      case e: java.lang.reflect.InvocationTargetException =>
        throw e.getCause
    }
  }

  /**
   * Build an [[InMemoryFileIndex]] for the given paths, resolving globs if necessary, without the
   * streaming metadata directory check.
   */
  def createFileIndex(
      sparkSession: SparkSession,
      options: CaseInsensitiveStringMap,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType]): PartitioningAwareFileIndex = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val globPathsEnabled =
      Option(options.get("globPaths")).map(v => java.lang.Boolean.parseBoolean(v)).getOrElse(true)
    val rootPathsSpecified = checkAndGlobPathIfNecessary(
      paths,
      hadoopConf,
      checkEmptyGlobPath = true,
      checkFilesExist = true,
      enableGlobbing = globPathsEnabled)
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(
      sparkSession,
      rootPathsSpecified,
      caseSensitiveMap,
      userSpecifiedSchema,
      fileStatusCache)
  }
}
