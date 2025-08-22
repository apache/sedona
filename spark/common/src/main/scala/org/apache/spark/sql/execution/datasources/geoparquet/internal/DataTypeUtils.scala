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
package org.apache.spark.sql.execution.datasources.geoparquet.internal

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object DataTypeUtils {

  /**
   * Check if `this` and `other` are the same data type when ignoring nullability
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  def sameType(left: DataType, right: DataType): Boolean = left.sameType(right)

  /**
   * Convert a StructField to a AttributeReference.
   */
  def toAttribute(field: StructField): AttributeReference =
    AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()

  /**
   * Convert a [[StructType]] into a Seq of [[AttributeReference]].
   */
  def toAttributes(schema: StructType): Seq[AttributeReference] = {
    schema.map(toAttribute)
  }

  /**
   * Check if the schema has any default values. We do this check before calling functions in
   * `ResolveDefaultColumns`, because `ResolveDefaultColumns` depends on lots of internal APIs of
   * Spark, and it could easily break on Databricks. If it has to break, let's make it only break
   * when existence default values are actually used.
   */
  def hasExistenceDefaultValues(schema: StructType): Boolean = {
    schema.exists(_.getExistenceDefaultValue.isDefined)
  }
}
