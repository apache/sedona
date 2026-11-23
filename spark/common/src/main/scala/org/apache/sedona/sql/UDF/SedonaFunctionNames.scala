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

import java.util.{Collections, Locale, WeakHashMap}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

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
  // Spark Connect sessions and the prefix each one reported. Reading it costs two round trips to
  // the server, and the server's prefix does not change, so each session is asked once.
  private val connectPrefixes =
    Collections.synchronizedMap(new WeakHashMap[SparkSession, String]())

  private def isValid(prefix: String): Boolean =
    prefix.isEmpty || ValidPrefix.pattern.matcher(prefix).matches()

  /**
   * Reads the key from the session conf (default "") and calls initialize(String, plainNames).
   * `plainNames` are the plain names of every function Sedona registers. Afterwards the key in
   * the session's conf holds the prefix in effect.
   */
  private[sedona] def initialize(
      sparkSession: SparkSession,
      plainNames: => Iterable[String]): Unit = {
    val requested = sparkSession.conf.get(PrefixConfigKey, "")
    val effective = initialize(requested, plainNames)
    // A Spark Connect client names the functions after the key's value in its session, so the
    // value must be the prefix the functions are registered under, not the one asked for.
    if (requested != effective) sparkSession.conf.set(PrefixConfigKey, effective)
  }

  /** initialize(requestedPrefix, plainNames) with no names to check against. */
  private[sedona] def initialize(requestedPrefix: String): Unit = {
    initialize(requestedPrefix, Nil)
    ()
  }

  /**
   * Validates, then fixes the effective prefix if none is fixed yet; otherwise warns on a
   * different value. A prefix is fixed only if it turns none of `plainNames` into another.
   * Returns the effective prefix.
   */
  private[sedona] def initialize(
      requestedPrefix: String,
      plainNames: => Iterable[String]): String = {
    if (!isValid(requestedPrefix)) {
      throw new IllegalArgumentException(
        s"Invalid value '$requestedPrefix' for $PrefixConfigKey: the prefix must be empty or " +
          "match ^[A-Za-z_][A-Za-z0-9_]*$")
    }
    // Outside the lock, as computing the names can initialize the catalog objects. Two threads
    // may both check; the first to take the lock fixes the prefix.
    if (fixedPrefix.isEmpty) requireNoCollision(requestedPrefix, plainNames)
    lock.synchronized {
      fixedPrefix match {
        case None =>
          fixedPrefix = Some(requestedPrefix)
          requestedPrefix
        case Some(effective) =>
          if (effective != requestedPrefix && warnedValues.add(requestedPrefix)) {
            logger.warn(
              s"$PrefixConfigKey is already fixed to '$effective' in this JVM; " +
                s"ignoring the requested value '$requestedPrefix'")
          }
          effective
      }
    }
  }

  /**
   * The fixed prefix. Until one is fixed, the key's value in the conf of the active session, else
   * of the default session (a stopped session is skipped), if that value is valid; otherwise "".
   * Reading it fixes nothing. So a DataFrame column built before the first registrator runs
   * already carries the name Sedona registers. Public so Python can read it over py4j.
   *
   * In a Spark Connect client no registrator runs, so the prefix is never fixed there. A Connect
   * session asks the server once for the prefix in effect; a failure of that request propagates.
   */
  def effectivePrefix: String = fixedPrefix match {
    case Some(prefix) => prefix
    case None => configuredPrefix
  }

  private def configuredPrefix: String =
    sessionPrefix(SparkSession.getActiveSession)
      .orElse(sessionPrefix(SparkSession.getDefaultSession))
      .getOrElse("")

  // The prefix `session` asks for, or None for no session or a stopped one: a stopped session
  // can stay the thread's active session. A Spark Connect session has no SparkContext, and
  // asking for one throws; such a session is never taken for stopped.
  private def sessionPrefix(session: Option[SparkSession]): Option[String] =
    session.flatMap { s =>
      // The cache first: a Connect client resolves every function name through here, and the
      // exception below costs more than a map lookup.
      Option(connectPrefixes.get(s)).orElse {
        Try(s.sparkContext) match {
          case Success(context) => if (context.isStopped) None else Some(confPrefix(s))
          case Failure(_) => Some(connectPrefix(s))
        }
      }
    }

  private def confPrefix(session: SparkSession): String =
    Try(session.conf.get(PrefixConfigKey, "")).toOption.filter(isValid).getOrElse("")

  private def connectPrefix(session: SparkSession): String =
    Option(connectPrefixes.get(session)).getOrElse {
      // With spark.sql.extensions the server registers Sedona for a session when it first
      // analyses a plan there, and registering puts the prefix in effect into the session's
      // conf. So analyse a trivial plan before reading the key.
      session.range(0).schema
      Try(session.conf.get(PrefixConfigKey, "")) match {
        case Success(value) =>
          val prefix = if (isValid(value)) value else ""
          connectPrefixes.put(session, prefix)
          prefix
        case Failure(_) => "" // Asked again next time.
      }
    }

  /** effectivePrefix + name. */
  def resolve(name: String): String = effectivePrefix + name

  /**
   * The prefix dropAll resolves names with: the fixed prefix, else the key's value in the conf of
   * `sparkSession` if that value is valid, else "". Reading it fixes nothing, so a drop before a
   * session sets the key does not stop a later registrator from fixing what that session asks
   * for. A value that initialize would reject for turning one of `plainNames` into another is
   * rejected here too, before anything is dropped.
   */
  private[sedona] def prefixToDrop(
      sparkSession: SparkSession,
      plainNames: => Iterable[String]): String = fixedPrefix match {
    case Some(prefix) => prefix
    case None =>
      val requested = sparkSession.conf.get(PrefixConfigKey, "")
      if (isValid(requested)) {
        requireNoCollision(requested, plainNames)
        requested
      } else ""
  }

  // Under a prefix such as ST_, GeometryType would be registered as ST_GeometryType, the plain
  // name of another Sedona function and maybe of a platform function: the prefix exists so that
  // Sedona never takes a plain name. Function names match ignoring case.
  private def requireNoCollision(prefix: String, plainNames: => Iterable[String]): Unit =
    if (prefix.nonEmpty) {
      val names = plainNames
      val byLowerCase = names.map(name => name.toLowerCase(Locale.ROOT) -> name).toMap
      names.iterator
        .map(name => (name, prefix + name))
        .find { case (_, prefixed) => byLowerCase.contains(prefixed.toLowerCase(Locale.ROOT)) }
        .foreach { case (name, prefixed) =>
          val other = byLowerCase(prefixed.toLowerCase(Locale.ROOT))
          throw new IllegalArgumentException(
            s"Invalid value '$prefix' for $PrefixConfigKey: Sedona would register $name as " +
              s"$prefixed, which is the name of the Sedona function $other. Choose a prefix " +
              "under which no Sedona function name becomes another.")
        }
    }

  private[sedona] def resetForTesting(): Unit = lock.synchronized {
    fixedPrefix = None
    warnedValues.clear()
    connectPrefixes.clear()
  }
}
