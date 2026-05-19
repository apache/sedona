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

import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.sedona_sql.UDT.{Box2DUDT, GeometryUDT}
import org.apache.spark.sql.types.DataType

class SedonaSqlAstBuilder extends SparkSqlAstBuilder {

  /**
   * Recognize Sedona UDT names (GEOMETRY, BOX2D) as primitive data types so SQL `CAST(... AS
   * geometry)` / `CAST(... AS box2d)` parse to the matching UDT.
   */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = {
    ctx.getText.toUpperCase() match {
      case "GEOMETRY" => GeometryUDT()
      case "BOX2D" => Box2DUDT
      case _ => super.visitPrimitiveDataType(ctx)
    }
  }
}
