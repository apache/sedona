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

import org.apache.sedona.common.geometrySerde.GeometrySerializer
import org.apache.sedona.common.raster.Constructors.fromArcInfoAsciiGrid
import org.apache.sedona.common.raster.Serde
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.sedona_sql.expressions.{ST_Buffer, ST_GeomFromText, ST_Point, ST_Union}
import org.apache.spark.sql.sedona_sql.expressions.raster.{RS_FromArcInfoAsciiGrid, RS_NumBands}
import org.geotools.coverage.grid.GridCoverage2D
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{atMost, mockStatic}

class SerdeAwareFunctionSpec extends TestBaseScala {

  describe("SerdeAwareFunction") {
    it("should save us some serialization and deserialization cost for geometries") {
      // Mock GeometrySerializer
      val factory = new GeometryFactory
      val stubGeom = factory.createPoint(new Coordinate(1, 2))
      val mocked = mockStatic(classOf[GeometrySerializer])
      mocked.when(() => GeometrySerializer.deserialize(any(classOf[Array[Byte]]))).thenReturn(stubGeom)
      mocked.when(() => GeometrySerializer.serialize(any(classOf[Geometry]))).thenReturn(Array[Byte](1, 2, 3))

      val expr = ST_Union(Seq(
        ST_Buffer(Seq(ST_GeomFromText(Seq(Literal("POINT (1 2)"), Literal(0))), Literal(1.0))),
        ST_Point(Seq(Literal(1.0), Literal(2.0), Literal(null)))
      ))

      try {
        // Evaluate an expression
        expr.eval(null)

        // Verify number of invocations
        mocked.verify(
          () => GeometrySerializer.deserialize(any(classOf[Array[Byte]])),
          atMost(0))
        mocked.verify(
          () => GeometrySerializer.serialize(any(classOf[Geometry])),
          atMost(1))
      } finally {
        // Undo the mock
        mocked.close()
      }
    }

    it("should save us some serialization and deserialization cost for rasters") {
      // Mock RasterSerializer
      val ascGrid =
        """
          |NCOLS 2
          |NROWS 2
          |XLLCORNER 378922
          |YLLCORNER 4072345
          |CELLSIZE 30
          |NODATA_VALUE 0
          |0 1 2 3
          |""".stripMargin
      val mocked = mockStatic(classOf[Serde])
      mocked.when(() => Serde.deserialize(any(classOf[Array[Byte]]))).thenReturn(fromArcInfoAsciiGrid(ascGrid.getBytes))
      mocked.when(() => Serde.serialize(any(classOf[GridCoverage2D]))).thenReturn(Array[Byte](1, 2, 3))

      val expr = RS_NumBands(Seq(
        RS_FromArcInfoAsciiGrid(Seq(Literal(ascGrid.getBytes)))
      ))

      try {
        // Evaluate an expression
        expr.eval(null)

        // Verify number of invocations
        mocked.verify(
          () => Serde.deserialize(any(classOf[Array[Byte]])),
          atMost(0))
        mocked.verify(
          () => Serde.serialize(any(classOf[GridCoverage2D])),
          atMost(1))
      } finally {
        // Undo the mock
        mocked.close()
      }
    }
  }
}
