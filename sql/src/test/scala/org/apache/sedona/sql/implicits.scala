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

import org.apache.spark.sql.DataFrame
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader

object implicits {

  implicit class DataFrameEnhancer(df: DataFrame) {
    def toSeq[T]: Seq[T] =
      df.collect().toSeq.map(element => element(0).asInstanceOf[T]).toList

    def toSeqOption[T]: Option[T] = {
      df.collect().headOption
        .map(element => if (element(0) != null) element(0).asInstanceOf[T] else None.asInstanceOf[T])
    }
  }

  implicit class GeometryFromString(wkt: String) {
    def toGeom: Geometry = {
      val wkbReader = new WKTReader()
      wkbReader.read(wkt)
    }

  }

}
