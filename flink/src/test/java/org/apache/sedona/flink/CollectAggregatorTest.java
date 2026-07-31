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
package org.apache.sedona.flink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.flink.expressions.Accumulators;
import org.apache.sedona.flink.expressions.Aggregators;
import org.junit.Test;

public class CollectAggregatorTest {

  private final Aggregators.ST_Collect_Aggr aggregate = new Aggregators.ST_Collect_Aggr();

  @Test
  public void testMergeGeographyAccumulators() throws Exception {
    Geography first = Constructors.geogFromWKT("POINT (170 10)", 4326);
    Geography second = Constructors.geogFromWKT("POINT (-170 10)", 4326);

    Accumulators.AccGeometryCollection target = aggregate.createAccumulator();
    aggregate.accumulate(target, first);

    Accumulators.AccGeometryCollection empty = aggregate.createAccumulator();
    Accumulators.AccGeometryCollection other = aggregate.createAccumulator();
    aggregate.accumulate(other, second);
    aggregate.accumulate(other, first);

    aggregate.merge(target, Arrays.asList(empty, other));

    Geography actual = (Geography) aggregate.getValue(target);
    Geography expected =
        org.apache.sedona.common.geography.Functions.createMultiGeography(
            new Geography[] {first, second, first});
    assertEquals(expected.toEWKT(), actual.toEWKT());
    assertEquals(3, org.apache.sedona.common.geography.Functions.numGeometries(actual));
    assertEquals(4326, actual.getSRID());
  }

  @Test
  public void testMixedSridRejectedByAccumulateAndMerge() throws Exception {
    Geography srid4326 = Constructors.geogFromWKT("POINT (1 2)", 4326);
    Geography srid3857 = Constructors.geogFromWKT("POINT (3 4)", 3857);

    Accumulators.AccGeometryCollection target = aggregate.createAccumulator();
    aggregate.accumulate(target, srid4326);

    IllegalArgumentException accumulateError =
        assertThrows(IllegalArgumentException.class, () -> aggregate.accumulate(target, srid3857));
    assertTrue(accumulateError.getMessage().contains("same SRID"));
    assertEquals(1, target.values.size());

    Accumulators.AccGeometryCollection other = aggregate.createAccumulator();
    aggregate.accumulate(other, srid3857);
    IllegalArgumentException mergeError =
        assertThrows(
            IllegalArgumentException.class, () -> aggregate.merge(target, Arrays.asList(other)));
    assertTrue(mergeError.getMessage().contains("same SRID"));
    assertEquals(1, target.values.size());
  }

  @Test
  public void testRetractAndResetAccumulator() throws Exception {
    Geography first = Constructors.geogFromWKT("POINT (1 2)", 4326);
    Geography second = Constructors.geogFromWKT("POINT (3 4)", 4326);

    Accumulators.AccGeometryCollection accumulator = aggregate.createAccumulator();
    aggregate.accumulate(accumulator, first);
    aggregate.accumulate(accumulator, first);
    aggregate.accumulate(accumulator, second);

    aggregate.retract(accumulator, first);

    Geography actual = (Geography) aggregate.getValue(accumulator);
    Geography expected =
        org.apache.sedona.common.geography.Functions.createMultiGeography(
            new Geography[] {first, second});
    assertEquals(expected.toEWKT(), actual.toEWKT());

    aggregate.resetAccumulator(accumulator);
    assertNull(aggregate.getValue(accumulator));
    assertTrue(accumulator.values.isEmpty());
    assertNull(accumulator.geography);
    assertEquals(0, accumulator.srid);

    Geography resetSrid = Constructors.geogFromWKT("POINT (5 6)", 3857);
    aggregate.accumulate(accumulator, resetSrid);
    assertEquals(3857, ((Geography) aggregate.getValue(accumulator)).getSRID());
  }
}
