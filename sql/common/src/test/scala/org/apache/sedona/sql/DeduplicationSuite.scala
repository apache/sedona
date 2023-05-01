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

import org.apache.spark.sql.functions.expr

class DeduplicationSuite extends TestBaseScala {

  describe("Sedona-SQL Deduplication") {
    it("Deduplicate of two joins in a single execution stage") {
      //See https://issues.apache.org/jira/browse/SEDONA-233
      import sparkSession.implicits._
      val left = Seq("POLYGON ((3 0, 3 3, 0 3, 0 0, 3 0))",
        "POLYGON ((4 1, 4 4, 1 4, 1 1, 4 1))",
        "POLYGON ((3 1, 3 4, 0 4, 0 1, 3 1))",
        "POLYGON ((4 0, 4 3, 1 3, 1 0, 4 0))")
        .toDF("left_geom")
        .withColumn("left_geom", expr("ST_GeomFromText(left_geom)"))
      val right = Seq("POLYGON ((4 0, 4 4, 0 4, 0 0, 4 0))")
        .toDF("right_geom")
        .withColumn("right_geom", expr("ST_GeomFromText(right_geom)"))
      val joined = left.join(right, expr("ST_Intersects(left_geom, right_geom)"))
      val result = joined.unionAll(joined)
      assert(result.count() == 8)
    }
  }
}
