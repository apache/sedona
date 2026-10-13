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
package org.apache.sedona.snowflake.snowsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.sedona.common.Constructors;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

/** Distance UDFs return NULL for an empty geometry, as the Spark and Flink functions do. */
public class EmptyGeometryDistanceTest {

  @Test
  public void distanceToAnEmptyGeometryIsNull() throws ParseException {
    Geometry line = Constructors.geomFromWKT("LINESTRING (0 0, 1 1, 2 2)", 0);
    Geometry other = Constructors.geomFromWKT("LINESTRING (0 0, 1 1, 3 3)", 0);
    Geometry empty = Constructors.geomFromWKT("POLYGON EMPTY", 0);

    byte[] lineWkb = GeometrySerde.serialize(line);
    byte[] emptyWkb = GeometrySerde.serialize(empty);
    assertNull(UDFs.ST_Distance(lineWkb, emptyWkb));
    assertNull(UDFs.ST_Distance(emptyWkb, lineWkb));
    assertNull(UDFs.ST_HausdorffDistance(lineWkb, emptyWkb));
    assertNull(UDFs.ST_HausdorffDistance(lineWkb, emptyWkb, 0.5));
    assertEquals(
        1.4142135623730951,
        UDFs.ST_HausdorffDistance(lineWkb, GeometrySerde.serialize(other)),
        1e-12);

    String lineJson = GeometrySerde.serGeoJson(line);
    String emptyJson = GeometrySerde.serGeoJson(empty);
    assertNull(UDFsV2.ST_Distance(lineJson, emptyJson));
    assertNull(UDFsV2.ST_Distance(emptyJson, lineJson));
    assertNull(UDFsV2.ST_HausdorffDistance(lineJson, emptyJson));
    assertNull(UDFsV2.ST_HausdorffDistance(lineJson, emptyJson, 0.5));
    assertEquals(
        1.4142135623730951,
        UDFsV2.ST_HausdorffDistance(lineJson, GeometrySerde.serGeoJson(other)),
        1e-12);
  }
}
