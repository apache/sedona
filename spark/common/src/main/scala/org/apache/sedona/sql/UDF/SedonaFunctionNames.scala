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

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Owns the name prefix of the Spark SQL functions Sedona registers. The first registrator to run
 * in the JVM fixes the effective prefix; later, different requests are ignored with a warning.
 */
object SedonaFunctionNames {
  val PrefixConfigKey: String = "spark.sedona.function.prefix"

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val ValidPrefix = "^[A-Za-z_][A-Za-z0-9_]*$".r

  // Guards fixing and resetting the prefix; reads go through the volatile field alone.
  private val lock = new Object
  // None means no prefix has been fixed yet; Some("") means it was fixed to the empty string.
  @volatile private var fixedPrefix: Option[String] = None
  private val warnedValues = mutable.Set[String]()

  private def isValid(prefix: String): Boolean =
    prefix.isEmpty || ValidPrefix.pattern.matcher(prefix).matches()

  /** Reads the key from the session conf (default "") and calls initialize(String). */
  def initialize(sparkSession: SparkSession): Unit =
    initialize(sparkSession.conf.get(PrefixConfigKey, ""))

  /**
   * Validates, then fixes the effective prefix if none is fixed yet; otherwise warns on a
   * different value.
   */
  private[sedona] def initialize(requestedPrefix: String): Unit = {
    if (!isValid(requestedPrefix)) {
      throw new IllegalArgumentException(
        s"Invalid value '$requestedPrefix' for $PrefixConfigKey: the prefix must be empty or " +
          "match ^[A-Za-z_][A-Za-z0-9_]*$")
    }
    lock.synchronized {
      fixedPrefix match {
        case None => fixedPrefix = Some(requestedPrefix)
        case Some(effective) =>
          if (effective != requestedPrefix && warnedValues.add(requestedPrefix)) {
            logger.warn(
              s"$PrefixConfigKey is already fixed to '$effective' in this JVM; " +
                s"ignoring the requested value '$requestedPrefix'")
          }
      }
    }
  }

  /**
   * The fixed prefix. Until one is fixed, the key's value in the conf of the active session, else
   * of the default session (a stopped session is skipped), if that value is valid; otherwise "".
   * Reading it fixes nothing. So a DataFrame column built before the first registrator runs
   * already carries the name Sedona registers. Public so Python can read it over py4j.
   */
  def effectivePrefix: String = fixedPrefix match {
    case Some(prefix) => prefix
    case None => configuredPrefix
  }

  // A stopped session can stay the thread's active session, so stopped ones are skipped.
  private def configuredPrefix: String =
    SparkSession.getActiveSession
      .filterNot(_.sparkContext.isStopped)
      .orElse(SparkSession.getDefaultSession.filterNot(_.sparkContext.isStopped))
      .map(_.conf.get(PrefixConfigKey, ""))
      .filter(isValid)
      .getOrElse("")

  /** effectivePrefix + name. */
  def resolve(name: String): String = effectivePrefix + name

  /**
   * The prefix dropAll resolves names with: the fixed prefix, else the key's value in the conf of
   * `sparkSession` if that value is valid, else "". Reading it fixes nothing, so a drop before a
   * session sets the key does not stop a later registrator from fixing what that session asks
   * for.
   */
  private[sedona] def prefixToDrop(sparkSession: SparkSession): String = fixedPrefix match {
    case Some(prefix) => prefix
    case None => Some(sparkSession.conf.get(PrefixConfigKey, "")).filter(isValid).getOrElse("")
  }

  private[sedona] def resetForTesting(): Unit = lock.synchronized {
    fixedPrefix = None
    warnedValues.clear()
  }
}
