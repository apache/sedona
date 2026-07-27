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
package org.apache.sedona.core.joinJudgement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.wrapper.KnnGeometryMetadata;
import org.apache.spark.util.LongAccumulator;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

public class InMemoryKNNJoinIteratorTest {
  private final GeometryFactory factory = new GeometryFactory();

  @Test
  public void queryLocalTieSemanticsPreserveDuplicateInputRows() {
    Point query = point(0, 0);
    Point duplicateA = point(1, 0);
    Point duplicateB = point(1, 0);

    List<Point> results =
        collect(query, Arrays.asList(duplicateA, duplicateB), DistanceMetric.EUCLIDEAN, true);

    assertEquals(2, results.size());
    assertNotSame(results.get(0), results.get(1));
  }

  @Test
  public void legacyConstructorRetainsGeometryEqualityDeduplication() {
    Point query = point(0, 0);
    Point duplicateA = point(1, 0);
    Point duplicateB = point(1, 0);
    STRtree index = index(Arrays.asList(duplicateA, duplicateB));

    InMemoryKNNJoinIterator<Point, Point> iterator =
        new InMemoryKNNJoinIterator<>(
            Collections.singletonList(query).iterator(),
            index,
            1,
            DistanceMetric.EUCLIDEAN,
            true,
            new LongAccumulator(),
            new LongAccumulator());

    assertEquals(1, collectValues(iterator).size());
  }

  @Test
  public void geographyRetainsHistoricalPlanarTieExpansion() {
    Point query = point(0, 90);
    List<Point> antipodes = Arrays.asList(point(0, -90), point(100, -90), point(100, -90));

    List<Point> results = collect(query, antipodes, DistanceMetric.HAVERSINE, false);

    assertEquals(1, results.size());
  }

  @Test
  public void queryLocalNoTiesSelectsLowestStableRowId() {
    Point query = point(0, 0);
    Point higherId = point(-1, 0);
    higherId.setUserData(KnnGeometryMetadata.wrap(9, "higher"));
    Point lowerId = point(1, 0);
    lowerId.setUserData(KnnGeometryMetadata.wrap(2, "lower"));

    List<Point> results =
        collect(query, Arrays.asList(higherId, lowerId), DistanceMetric.EUCLIDEAN, false, true);

    assertEquals(Collections.singletonList(lowerId), results);
  }

  @Test
  public void queryLocalNoTiesDoesNotRequireMetadataWithoutTieAmbiguity() {
    Point query = point(0, 0);
    Point candidate = point(1, 0);

    List<Point> results =
        collect(query, Collections.singletonList(candidate), DistanceMetric.EUCLIDEAN, false, true);

    assertEquals(Collections.singletonList(candidate), results);
  }

  @Test
  public void stableMetadataWrappingIsIdempotentAndRobustToOlderNesting() {
    Object originalRow = new Object();
    KnnGeometryMetadata metadata = KnnGeometryMetadata.wrap(4, originalRow);

    assertSame(metadata, KnnGeometryMetadata.wrap(7, metadata));
    assertSame(originalRow, new KnnGeometryMetadata(8, metadata).getOriginalUserData());
  }

  @Test
  public void topologicalEqualityUsesSafeShortCircuits() {
    Point point = point(0, 0);
    assertTrue(KnnJoinIndexJudgement.areTopologicallyEqual(point, point));
    assertFalse(KnnJoinIndexJudgement.areTopologicallyEqual(point, point(1, 0)));

    LineString forward =
        factory.createLineString(new Coordinate[] {new Coordinate(0, 0), new Coordinate(1, 0)});
    LineString reverse =
        factory.createLineString(new Coordinate[] {new Coordinate(1, 0), new Coordinate(0, 0)});
    assertTrue(KnnJoinIndexJudgement.areTopologicallyEqual(forward, reverse));
  }

  private List<Point> collect(
      Point query,
      List<Point> candidates,
      DistanceMetric distanceMetric,
      boolean queryLocalTieSemantics) {
    return collect(query, candidates, distanceMetric, true, queryLocalTieSemantics);
  }

  private List<Point> collect(
      Point query,
      List<Point> candidates,
      DistanceMetric distanceMetric,
      boolean includeTies,
      boolean queryLocalTieSemantics) {
    InMemoryKNNJoinIterator<Point, Point> iterator =
        new InMemoryKNNJoinIterator<>(
            Collections.singletonList(query).iterator(),
            index(candidates),
            1,
            distanceMetric,
            includeTies,
            false,
            queryLocalTieSemantics,
            new LongAccumulator(),
            new LongAccumulator());
    return collectValues(iterator);
  }

  private List<Point> collectValues(InMemoryKNNJoinIterator<Point, Point> iterator) {
    List<Point> results = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<Point, Point> pair = iterator.next();
      results.add(pair.getValue());
    }
    return results;
  }

  private STRtree index(List<Point> points) {
    STRtree index = new STRtree();
    for (Point point : points) {
      index.insert(point.getEnvelopeInternal(), point);
    }
    index.build();
    return index;
  }

  private Point point(double x, double y) {
    return factory.createPoint(new Coordinate(x, y));
  }
}
