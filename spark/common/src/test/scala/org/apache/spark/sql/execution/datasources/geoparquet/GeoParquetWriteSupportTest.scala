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
package org.apache.spark.sql.execution.datasources.geoparquet

import org.apache.spark.sql.execution.datasources.geoparquet.GeoParquetWriteSupport.GeometryColumnBoundingBox
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite

class GeoParquetWriteSupportTest extends AnyFunSuite {
  test("empty geometries do not contribute a column bounding box") {
    val factory = new GeometryFactory()
    val bbox = new GeometryColumnBoundingBox()
    bbox.update(factory.createPoint())
    bbox.update(factory.createLineString())
    bbox.update(factory.createPolygon())
    assert(bbox.minX == Double.PositiveInfinity)
    assert(bbox.minY == Double.PositiveInfinity)
    assert(bbox.maxX == Double.NegativeInfinity)
    assert(bbox.maxY == Double.NegativeInfinity)
  }

  test("empty geometries do not extend a nonempty column bounding box") {
    val factory = new GeometryFactory()
    val bbox = new GeometryColumnBoundingBox()
    bbox.update(factory.createPoint(new Coordinate(10, 20)))
    bbox.update(factory.createPolygon())
    assert(Seq(bbox.minX, bbox.minY, bbox.maxX, bbox.maxY) == Seq(10, 20, 10, 20))
  }
}
