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
import org.slf4j.{Logger, LoggerFactory}

import java.util.{Collections, Locale, WeakHashMap}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.util.Try

/**
 * Owns the name prefix of the Spark SQL functions Sedona registers. The first registrator to run
 * in the JVM fixes the effective prefix; later, different requests are ignored with a warning.
 */
object SedonaFunctionNames {
  val PrefixConfigKey: String = "spark.sedona.function.prefix"

  /**
   * A Sedona function that no platform has. A Spark Connect client finds the prefix in effect on
   * the server in the name the server lists this function under, so Catalog must keep it.
   */
  private[sedona] val MarkerFunctionName: String =
    "__sedona_internal_coverage_invalid_edges_for_target"

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val ValidPrefix = "^[A-Za-z_][A-Za-z0-9_]*$".r

  // Guards fixing and resetting the prefix; reads go through the volatile field alone.
  private val lock = new Object
  // None means no prefix has been fixed yet; Some("") means it was fixed to the empty string.
  @volatile private var fixedPrefix: Option[String] = None
  // Set when Sedona is loaded through spark.sql.extensions: the key's value in the application's
  // Spark config ("" without the key). It then decides the prefix, whatever a session asks for.
  @volatile private var applicationPrefix: Option[String] = None
  private val warnedValues = mutable.Set[String]()
  // Spark Connect sessions and the prefix each one's server reported. Asking costs round trips
  // to the server, and the server's prefix does not change, so each session asks once.
  private val connectPrefixes =
    Collections.synchronizedMap(new WeakHashMap[SparkSession, String]())
  // The classes of the sessions that turned out to be Spark Connect sessions.
  private val connectSessionClasses = ConcurrentHashMap.newKeySet[Class[_]]()

  private def isValid(prefix: String): Boolean =
    prefix.isEmpty || ValidPrefix.pattern.matcher(prefix).matches()

  private def requireValid(prefix: String): Unit =
    if (!isValid(prefix)) {
      throw new IllegalArgumentException(
        s"Invalid value '$prefix' for $PrefixConfigKey: the prefix must be empty or " +
          "match ^[A-Za-z_][A-Za-z0-9_]*$")
    }

  /**
   * Makes the application's Spark config decide the prefix. SedonaSqlExtensions calls this when
   * Spark loads it. With spark.sql.extensions Sedona registers for every session, also for the
   * session of a Spark Connect client, and a client must not choose the names for the whole
   * server by setting the key on its own session.
   */
  private[sedona] def useApplicationConf(conf: SparkConf): Unit =
    applicationPrefix = Some(conf.get(PrefixConfigKey, ""))

  // The prefix `sparkSession` asks for: the application's if that decides, else the session's.
  private def requestedPrefix(sparkSession: SparkSession): String =
    applicationPrefix.getOrElse(sparkSession.conf.get(PrefixConfigKey, ""))

  /**
   * Calls initialize(String, plainNames) with the key's value in the application's Spark config
   * if Sedona was loaded through spark.sql.extensions, else with its value in the session conf
   * (default ""). `plainNames` are the plain names of every function Sedona registers.
   */
  private[sedona] def initialize(
      sparkSession: SparkSession,
      plainNames: => Iterable[String]): Unit = {
    val effective = initialize(requestedPrefix(sparkSession), plainNames)
    // The session may ask for something the application's config overrules.
    val asked = sparkSession.conf.get(PrefixConfigKey, "")
    if (asked != effective) lock.synchronized(warnIgnored(effective, asked))
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
    requireValid(requestedPrefix)
    // Outside the lock, as computing the names can initialize the catalog objects. Two threads
    // may both check; the first to take the lock fixes the prefix.
    if (fixedPrefix.isEmpty) requireNoCollision(requestedPrefix, plainNames)
    lock.synchronized {
      fixedPrefix match {
        case None =>
          fixedPrefix = Some(requestedPrefix)
          requestedPrefix
        case Some(effective) =>
          if (effective != requestedPrefix) warnIgnored(effective, requestedPrefix)
          effective
      }
    }
  }

  // Call with the lock held. One warning per ignored value.
  private def warnIgnored(effective: String, ignored: String): Unit =
    if (warnedValues.add(ignored)) {
      logger.warn(
        s"$PrefixConfigKey is already fixed to '$effective' in this JVM; " +
          s"ignoring the requested value '$ignored'")
    }

  /**
   * The prefix of the names Sedona's functions are registered under.
   *
   * If the active session is a Spark Connect session, the prefix in effect on its server: no
   * registrator runs in a Connect client, and a prefix fixed by an earlier classic session of
   * this JVM says nothing about the server. The session asks the server once; a failure of that
   * request propagates.
   *
   * Otherwise the fixed prefix. Until one is fixed: the key's value in the application's Spark
   * config if Sedona was loaded through spark.sql.extensions, else in the conf of the active
   * session, else of the default session (a stopped session is skipped), if that value is valid;
   * otherwise "". Reading it fixes nothing. So a DataFrame column built before the first
   * registrator runs already carries the name Sedona registers.
   *
   * Public so Python can read it over py4j.
   */
  def effectivePrefix: String = {
    val active = SparkSession.getActiveSession
    active.filter(isConnect) match {
      case Some(session) => connectPrefix(session)
      case None => fixedPrefix.getOrElse(configuredPrefix(active))
    }
  }

  private def configuredPrefix(active: Option[SparkSession]): String =
    applicationPrefix match {
      case Some(prefix) => if (isValid(prefix)) prefix else ""
      case None =>
        sessionPrefix(active).orElse(sessionPrefix(SparkSession.getDefaultSession)).getOrElse("")
    }

  // A Spark Connect session has no SparkContext, and asking for one throws. The session's class
  // is remembered, so the exception is paid once and not on every function name.
  private def isConnect(session: SparkSession): Boolean =
    connectSessionClasses.contains(session.getClass) || {
      val connect = Try(session.sparkContext).isFailure
      if (connect) connectSessionClasses.add(session.getClass)
      connect
    }

  // The prefix `session` asks for, or None for no session or a stopped one: a stopped session
  // can stay the thread's active session.
  private def sessionPrefix(session: Option[SparkSession]): Option[String] =
    session.flatMap { s =>
      if (isConnect(s)) Some(connectPrefix(s))
      else if (s.sparkContext.isStopped) None
      else Some(Try(s.conf.get(PrefixConfigKey, "")).toOption.filter(isValid).getOrElse(""))
    }

  // The server's function registry is the one place that says under which names Sedona's
  // functions are registered, and a client cannot write to it as it can to its session's conf.
  // Listing the function also makes a server that loads Sedona through spark.sql.extensions
  // register Sedona for this session first. A failure propagates: a guessed plain name could
  // call a platform's function.
  private def connectPrefix(session: SparkSession): String =
    Option(connectPrefixes.get(session)).getOrElse {
      val listed =
        session.sql(s"SHOW FUNCTIONS LIKE '*$MarkerFunctionName'").collect().map(_.getString(0))
      val prefix = prefixOfListedNames(listed)
      connectPrefixes.put(session, prefix)
      prefix
    }

  /**
   * The prefix in the names a server lists for `SHOW FUNCTIONS LIKE '*<marker>'`: "" when it
   * lists none (no Sedona there, or a Sedona from before the prefix). Spark lists function names
   * in lower case, and resolves them ignoring case.
   */
  private[sedona] def prefixOfListedNames(listed: Iterable[String]): String = {
    val marker = MarkerFunctionName.toLowerCase(Locale.ROOT)
    val prefixes = listed
      .map(_.toLowerCase(Locale.ROOT))
      .filter(_.endsWith(marker))
      .map(_.dropRight(marker.length))
    if (prefixes.isEmpty) "" else prefixes.minBy(_.length)
  }

  /** effectivePrefix + name. */
  def resolve(name: String): String = effectivePrefix + name

  /**
   * The prefix dropAll resolves names with: the fixed prefix, else the value initialize would
   * take for `sparkSession`. Reading it fixes nothing, so a drop before a session sets the key
   * does not stop a later registrator from fixing what that session asks for. A value that
   * initialize would reject is rejected here too, before anything is dropped: dropping under
   * another prefix, or under none, could remove a platform's function.
   */
  private[sedona] def prefixToDrop(
      sparkSession: SparkSession,
      plainNames: => Iterable[String]): String = fixedPrefix match {
    case Some(prefix) => prefix
    case None =>
      val requested = requestedPrefix(sparkSession)
      requireValid(requested)
      requireNoCollision(requested, plainNames)
      requested
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
    applicationPrefix = None
    warnedValues.clear()
    connectPrefixes.clear()
  }
}
