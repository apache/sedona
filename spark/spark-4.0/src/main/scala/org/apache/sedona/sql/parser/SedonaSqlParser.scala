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
package org.apache.sedona.sql.parser

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser

class SedonaSqlParser(delegate: ParserInterface) extends SparkSqlParser {

  // The parser builder for the Sedona SQL AST
  val parserBuilder = new SedonaSqlAstBuilder

  /**
   * Parse the SQL text and return the logical plan. This method first attempts to use the
   * delegate parser to parse the SQL text. If the delegate parser fails (throws an exception), it
   * falls back to using the Sedona SQL parser.
   *
   * @param sqlText
   *   The SQL text to be parsed.
   * @return
   *   The parsed logical plan.
   */
  override def parsePlan(sqlText: String): LogicalPlan =
    try {
      delegate.parsePlan(sqlText)
    } catch {
      case _: Exception =>
        parse(sqlText) { parser =>
          parserBuilder.visit(parser.singleStatement())
        }.asInstanceOf[LogicalPlan]
    }
}
