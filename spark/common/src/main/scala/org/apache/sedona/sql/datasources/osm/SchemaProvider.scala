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
package org.apache.sedona.sql.datasources.osm

import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, MapType, StringType, StructField, StructType}

trait SchemaProvider {
  def schema: StructType =
    StructType(
      Seq(
        StructField("id", LongType),
        StructField("kind", StringType),
        StructField(
          "location",
          StructType(
            Seq(
              StructField("longitude", DoubleType, nullable = false),
              StructField("latitude", DoubleType, nullable = false))),
          nullable = true),
        StructField(
          "tags",
          MapType(StringType, StringType, valueContainsNull = true),
          nullable = true),
        StructField("refs", ArrayType(LongType), nullable = true),
        StructField("ref_roles", ArrayType(StringType), nullable = true),
        StructField("ref_types", ArrayType(StringType), nullable = true)))
}
