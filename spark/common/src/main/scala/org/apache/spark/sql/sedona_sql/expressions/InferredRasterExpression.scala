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

import org.apache.sedona.sql.utils.GeoToolsCoverageAvailability.isGeoToolsAvailable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{ArrayType, UserDefinedType}

import scala.reflect.runtime.universe.{Type, typeOf}

object InferredRasterExpression {
  def isRasterType(t: Type): Boolean =
    isGeoToolsAvailable && InferrableRasterTypes.isRasterType(t)

  def isRasterArrayType(t: Type): Boolean =
    isGeoToolsAvailable && InferrableRasterTypes.isRasterArrayType(t)

  def rasterUDT: UserDefinedType[_] = if (isGeoToolsAvailable) {
    InferrableRasterTypes.rasterUDT
  } else {
    null
  }

  def rasterUDTArray: ArrayType = if (isGeoToolsAvailable) {
    InferrableRasterTypes.rasterUDTArray
  } else {
    null
  }

  val rasterExtractor: Expression => InternalRow => Any = if (isGeoToolsAvailable) {
    InferrableRasterTypes.rasterExtractor
  } else { _ => _ =>
    null
  }

  val rasterSerializer: Any => Any = if (isGeoToolsAvailable) {
    InferrableRasterTypes.rasterSerializer
  } else { (_: Any) =>
    null
  }

  val rasterArraySerializer: Any => Any = if (isGeoToolsAvailable) {
    InferrableRasterTypes.rasterArraySerializer
  } else { (_: Any) =>
    null
  }
}
