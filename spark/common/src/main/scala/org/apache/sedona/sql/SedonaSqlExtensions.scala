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
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.parser.ParserFactory
import org.slf4j.{Logger, LoggerFactory}

class SedonaSqlExtensions extends (SparkSessionExtensions => Unit) {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val enableParser =
    SparkContext.getOrCreate().getConf.get("spark.sedona.enableParserExtension", "true").toBoolean

  def apply(e: SparkSessionExtensions): Unit = {
    e.injectCheckRule(spark => {
      SedonaContext.create(spark)
      _ => ()
    })

    // Inject Sedona SQL parser
    if (enableParser) {
      // Try to inject the Sedona SQL parser but gracefully handle initialization failures.
      // This prevents extension loading errors from causing the SparkSession initialization to fail,
      // allowing the application to continue running without the Sedona parser extension.
      // Common failures include version incompatibilities between Spark and Sedona.
      try {
        e.injectParser { case (_, parser) =>
          ParserFactory.getParser("org.apache.sedona.sql.parser.SedonaSqlParser", parser)
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to inject Sedona SQL parser: ${ex.getMessage}", ex)
        // Skip parser injection
      }
    }
  }
}
