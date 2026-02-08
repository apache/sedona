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

// We use constant 5000 for Sedona UDFs, 200 is Apache Spark scalar UDF
object PythonEvalType {
  val SQL_SCALAR_SEDONA_UDF = 5200
  val SEDONA_UDF_TYPE_CONSTANT = 5000

  // sedona db eval types
  val SQL_SCALAR_SEDONA_DB_UDF = 6200
  val SQL_SCALAR_SEDONA_DB_SPEEDUP_UDF = 6201
  val SEDONA_DB_UDF_TYPE_CONSTANT = 6000

  def toString(pythonEvalType: Int): String = pythonEvalType match {
    case SQL_SCALAR_SEDONA_UDF => "SQL_SCALAR_GEO_UDF"
    case SQL_SCALAR_SEDONA_DB_UDF => "SQL_SCALAR_SEDONA_DB_UDF"
  }

  def evals(): Set[Int] =
    Set(SQL_SCALAR_SEDONA_UDF, SQL_SCALAR_SEDONA_DB_UDF, SQL_SCALAR_SEDONA_DB_SPEEDUP_UDF)
}
