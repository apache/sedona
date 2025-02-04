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
package org.apache.spark.sql.execution.datasource.stac

import java.time.LocalDateTime

/* A temporal filter that can be pushed down to the STAC data source.
 * It wraps a [[TemporalFilter]] and provides a simple string representation.
 * @param temporalFilter
 */
trait TemporalFilter {
  def evaluate(columns: Map[String, LocalDateTime]): Boolean
  def simpleString: String
}

object TemporalFilter {

  case class AndFilter(left: TemporalFilter, right: TemporalFilter) extends TemporalFilter {
    override def evaluate(columns: Map[String, LocalDateTime]): Boolean = {
      left.evaluate(columns) && right.evaluate(columns)
    }

    override def simpleString: String = s"(${left.simpleString}) AND (${right.simpleString})"
  }

  case class OrFilter(left: TemporalFilter, right: TemporalFilter) extends TemporalFilter {
    override def evaluate(columns: Map[String, LocalDateTime]): Boolean =
      left.evaluate(columns) || right.evaluate(columns)
    override def simpleString: String = s"(${left.simpleString}) OR (${right.simpleString})"
  }

  case class LessThanFilter(columnName: String, value: LocalDateTime) extends TemporalFilter {
    override def evaluate(columns: Map[String, LocalDateTime]): Boolean = {
      columns.get(columnName).exists(_ isBefore value)
    }
    override def simpleString: String = s"$columnName < $value"
  }

  case class GreaterThanFilter(columnName: String, value: LocalDateTime) extends TemporalFilter {
    override def evaluate(columns: Map[String, LocalDateTime]): Boolean = {
      columns.get(columnName).exists(_ isAfter value)
    }
    override def simpleString: String = s"$columnName > $value"
  }

  case class EqualFilter(columnName: String, value: LocalDateTime) extends TemporalFilter {
    override def evaluate(columns: Map[String, LocalDateTime]): Boolean = {
      columns.get(columnName).exists(_ isEqual value)
    }
    override def simpleString: String = s"$columnName = $value"
  }
}
