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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.SparkFiles
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URI

/**
 * Resolves libpostal data directory paths. When the configured data directory points to a remote
 * filesystem (HDFS, S3, GCS, ABFS, etc.), the data is expected to have been distributed to
 * executors via `SparkContext.addFile()` and is resolved through `SparkFiles.get()`.
 */
object LibPostalDataLoader {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Resolve the data directory to a local filesystem path. If the configured path already points
   * to the local filesystem, it is returned as-is. If it points to a remote filesystem, the data
   * is looked up via Spark's `SparkFiles` mechanism (the user must have called
   * `sc.addFile(remotePath, recursive = true)` before running queries).
   *
   * @param configuredDir
   *   the data directory path from Sedona configuration (may be local or remote)
   * @return
   *   a local filesystem path suitable for jpostal
   */
  def resolveDataDir(configuredDir: String): String = {
    if (isRemotePath(configuredDir)) {
      resolveFromSparkFiles(configuredDir)
    } else {
      normalizeLocalPath(configuredDir)
    }
  }

  /**
   * Normalize a local path. Converts `file:` URIs (e.g. `file:///tmp/libpostal`) to plain
   * filesystem paths (`/tmp/libpostal`) so that jpostal receives a path it can use directly.
   * Non-URI paths are returned unchanged.
   */
  private[expressions] def normalizeLocalPath(path: String): String = {
    try {
      val uri = new URI(path)
      if (uri.getScheme != null && uri.getScheme.equalsIgnoreCase("file")) {
        new File(uri).getAbsolutePath
      } else {
        path
      }
    } catch {
      case _: Exception => path
    }
  }

  /**
   * Determine whether a path string refers to a remote (non-local) filesystem.
   */
  def isRemotePath(path: String): Boolean = {
    try {
      val uri = new URI(path)
      val scheme = uri.getScheme
      scheme != null && !scheme.equalsIgnoreCase("file") && scheme.length > 1
    } catch {
      case _: Exception => false
    }
  }

  /**
   * Resolve a remote data directory via Spark's file distribution mechanism. Extracts the
   * basename (last path component) from the remote URI and looks it up through `SparkFiles.get`.
   * The user must have previously called `sc.addFile(remotePath, recursive = true)`.
   *
   * @throws IllegalStateException
   *   if the data directory was not found via SparkFiles
   */
  private def resolveFromSparkFiles(remotePath: String): String = {
    val basename = extractBasename(remotePath)

    try {
      val localPath = SparkFiles.get(basename)
      val localFile = new File(localPath)

      if (localFile.exists() && localFile.isDirectory) {
        logger.info(
          "Resolved libpostal data from SparkFiles: {} -> {}",
          remotePath: Any,
          localPath: Any)
        ensureTrailingSlash(localPath)
      } else {
        throw new IllegalStateException(
          s"libpostal data directory '$basename' was not found via SparkFiles. " +
            "Please call sc.addFile(\"" + remotePath + "\", recursive = true) before running libpostal queries.")
      }
    } catch {
      case e: IllegalStateException => throw e
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to resolve libpostal data from SparkFiles for '$remotePath'. " +
            "Please call sc.addFile(\"" + remotePath + "\", recursive = true) before running libpostal queries.",
          e)
    }
  }

  /**
   * Extract the basename (last path component) from a URI string. Trailing slashes are stripped
   * before extracting the last component.
   */
  private[expressions] def extractBasename(path: String): String = {
    val trimmed = path.replaceAll("/+$", "")
    val uri = new URI(trimmed)
    val uriPath = uri.getPath
    if (uriPath == null || uriPath.isEmpty) {
      trimmed.split("/").last
    } else {
      uriPath.split("/").last
    }
  }

  private def ensureTrailingSlash(path: String): String = {
    if (path.endsWith("/") || path.endsWith(File.separator)) path
    else path + File.separator
  }
}
