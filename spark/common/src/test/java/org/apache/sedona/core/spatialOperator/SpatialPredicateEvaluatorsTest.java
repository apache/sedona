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
package org.apache.sedona.core.spatialOperator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class SpatialPredicateEvaluatorsTest {
  @Test
  public void testCrossesGeometryCollections() throws ParseException {
    SpatialPredicateEvaluators.SpatialPredicateEvaluator evaluator =
        SpatialPredicateEvaluators.create(SpatialPredicate.CROSSES);
    WKTReader reader = new WKTReader();
    Geometry line = reader.read("LINESTRING (0 0, 2 2)");
    String[] collections = {
      "GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (0 2, 2 0))",
      "GEOMETRYCOLLECTION (LINESTRING (1 1, 3 3))",
      "GEOMETRYCOLLECTION EMPTY"
    };
    boolean[] expected = {true, false, false};
    for (int i = 0; i < collections.length; i++) {
      Geometry collection = reader.read(collections[i]);
      assertEquals(expected[i], evaluator.eval(collection, line));
      assertEquals(expected[i], evaluator.eval(line, collection));
      assertEquals(expected[i], evaluator.eval(PreparedGeometryFactory.prepare(collection), line));
      assertEquals(expected[i], evaluator.eval(PreparedGeometryFactory.prepare(line), collection));
    }
  }
}
