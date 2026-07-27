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
package org.apache.sedona.core.spatialPartitioning.quadtree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.sedona.core.spatialPartitioning.QuadtreePartitioning;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

public class QuadTreePartitioningTest {

  private final GeometryFactory factory = new GeometryFactory();

  /**
   * Verifies that data skew doesn't cause java.lang.StackOverflowError in StandardQuadTree.insert
   */
  @Test
  public void testDataSkew() throws Exception {

    // Create an artificially skewed data set of identical envelopes
    final Point point = factory.createPoint(new Coordinate(0, 0));

    final List<Envelope> samples = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      samples.add(point.getEnvelopeInternal());
    }

    final Envelope extent = new Envelope(0, 1, 0, 1);

    // Make sure Quad-tree is built successfully without throwing
    // java.lang.StackOverflowError
    QuadtreePartitioning partitioning = new QuadtreePartitioning(samples, extent, 10);
    Assert.assertNotNull(partitioning.getPartitionTree());
  }

  @Test
  public void testFullGeometryKeysMatchPlacedPartitions() throws Exception {
    ExtendedQuadTree<Integer> source = createExtendedTree(true);

    ExtendedQuadTree<Integer> queryTree = new ExtendedQuadTree<>(source, true, true);
    LineString line =
        factory.createLineString(new Coordinate[] {new Coordinate(1, 1), new Coordinate(9, 9)});
    Set<Integer> placedPartitions = new HashSet<>();
    Iterator<Tuple2<Integer, Geometry>> placements = queryTree.placeObject(line);
    while (placements.hasNext()) {
      placedPartitions.add(placements.next()._1());
    }

    assertTrue(placedPartitions.size() > 1);
    assertEquals(placedPartitions, queryTree.getKeys(line));
  }

  @Test
  public void testLegacyKeysPreserveHistoricalRouting() throws Exception {
    ExtendedQuadTree<Integer> source = createExtendedTree(false);
    Point point = factory.createPoint(new Coordinate(5, 5));

    assertEquals(source.getQuadTree().getKeys(point), source.getKeys(point));

    ExtendedQuadTree<Integer> queryTree = new ExtendedQuadTree<>(source, true, false);
    Set<Integer> expectedKeys =
        new HashSet<>(source.getSpatialExpandedBoundaryIndex().query(point.getEnvelopeInternal()));
    assertEquals(expectedKeys, queryTree.getKeys(point));
  }

  private ExtendedQuadTree<Integer> createExtendedTree(boolean useKnnSamples) throws Exception {
    ExtendedQuadTree<Integer> source = new ExtendedQuadTree<>(new Envelope(0, 10, 0, 10), 4);
    source.insert(new Envelope(new Coordinate(1, 1)));
    source.insert(new Envelope(new Coordinate(1, 9)));
    source.insert(new Envelope(new Coordinate(9, 1)));
    source.insert(new Envelope(new Coordinate(9, 9)));
    source.build(1, false, useKnnSamples);
    return source;
  }
}
