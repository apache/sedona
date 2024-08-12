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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.parser.ParserFactory

object ParserRegistrator {

  /**
   * Register the custom Sedona Spark parser
   * @param sparkSession
   */
  def register(sparkSession: SparkSession): Unit = {
    // try to register the parser with the new constructor for spark 3.1 and above
    try {
      val parserClassName = "org.apache.sedona.sql.parser.SedonaSqlParser"
      val delegate: ParserInterface = sparkSession.sessionState.sqlParser

      val parser = ParserFactory.getParser(parserClassName, delegate)
      val field = sparkSession.sessionState.getClass.getDeclaredField("sqlParser")
      field.setAccessible(true)
      field.set(sparkSession.sessionState, parser)
      return // return if the new constructor is available
    } catch {
      case _: Exception =>
    }

    // try to register the parser with the legacy constructor for spark 3.0
    try {
      val parserClassName = "org.apache.sedona.sql.parser.SedonaSqlParser"
      val delegate: ParserInterface = sparkSession.sessionState.sqlParser

      val parser =
        ParserFactory.getParser(parserClassName, sparkSession.sessionState.conf, delegate)
      val field = sparkSession.sessionState.getClass.getDeclaredField("sqlParser")
      field.setAccessible(true)
      field.set(sparkSession.sessionState, parser)
    } catch {
      case _: Exception =>
    }
  }
}
