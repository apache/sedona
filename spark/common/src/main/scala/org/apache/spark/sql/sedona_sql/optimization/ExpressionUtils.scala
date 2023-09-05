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

package org.apache.spark.sql.sedona_sql.optimization

import org.apache.spark.sql.catalyst.expressions.{And, Expression}

/**
 * This class contains helper methods for transforming catalyst expressions.
 */
object ExpressionUtils {
  /**
   * This is a polyfill for running on Spark 3.0 while compiling against Spark 3.3. We'd really like to mixin
   * `PredicateHelper` here, but the class hierarchy of `PredicateHelper` has changed between Spark 3.0 and 3.3 so
   * it would raise `java.lang.ClassNotFoundException: org.apache.spark.sql.catalyst.expressions.AliasHelper`
   * at runtime on Spark 3.0.
   *
   * @param condition filter condition to split
   * @return A list of conjunctive conditions
   */
  def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }
}
