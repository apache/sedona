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
package org.apache.sedona.stats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT

private[stats] object Util {
  def getGeometryColumnName(dataframe: DataFrame): String = {
    val geomFields = dataframe.schema.fields.filter(_.dataType == GeometryUDT)

    if (geomFields.isEmpty)
      throw new IllegalArgumentException(
        "No GeometryType column found. Provide a dataframe containing a geometry column.")

    if (geomFields.length == 1)
      return geomFields.head.name

    if (geomFields.length > 1 && !geomFields.exists(_.name == "geometry"))
      throw new IllegalArgumentException(
        "Multiple GeometryType columns found. Provide the column name as an argument.")

    "geometry"
  }
}
