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
package org.apache.sedona.core.spatialRDD;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialPartitioning.QuadTreeRTPartitioner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

// TODO: Auto-generated Javadoc

/** The Class PointRDDTest. */
public class PointRDDTest extends SpatialRDDTestBase {
  /** Once executed before all. */
  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize(PointRDDTest.class.getSimpleName(), "point.test.properties");
  }

  /** Tear down. */
  @AfterClass
  public static void TearDown() {
    sc.stop();
  }

  /**
   * Test constructor.
   *
   * @throws Exception the exception
   */
  @Test
  public void testConstructor() {
    PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
    spatialRDD.analyze();
    assertEquals(inputCount, spatialRDD.approximateTotalCount);
    assertEquals(inputBoundary, spatialRDD.boundaryEnvelope);
    assert spatialRDD
        .rawSpatialRDD
        .take(9)
        .get(0)
        .getUserData()
        .equals("testattribute0\ttestattribute1\ttestattribute2");
    assert spatialRDD
        .rawSpatialRDD
        .take(9)
        .get(2)
        .getUserData()
        .equals("testattribute0\ttestattribute1\ttestattribute2");
    assert spatialRDD
        .rawSpatialRDD
        .take(9)
        .get(4)
        .getUserData()
        .equals("testattribute0\ttestattribute1\ttestattribute2");
    assert spatialRDD
        .rawSpatialRDD
        .take(9)
        .get(8)
        .getUserData()
        .equals("testattribute0\ttestattribute1\ttestattribute2");
  }

  @Test
  public void testEmptyConstructor() throws Exception {
    PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
    spatialRDD.buildIndex(IndexType.RTREE, false);
    // Create an empty spatialRDD and manually assemble it
    PointRDD spatialRDDcopy = new PointRDD();
    spatialRDDcopy.rawSpatialRDD = spatialRDD.rawSpatialRDD;
    spatialRDDcopy.indexedRawRDD = spatialRDD.indexedRawRDD;
    spatialRDDcopy.analyze();
  }

  /**
   * Test build index without set grid.
   *
   * @throws Exception the exception
   */
  @Test
  public void testBuildIndexWithoutSetGrid() throws Exception {
    PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
    spatialRDD.buildIndex(IndexType.RTREE, false);
  }

  /**
   * Test build rtree index.
   *
   * @throws Exception the exception
   */
  @Test
  public void testBuildRtreeIndex() throws Exception {
    PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(gridType);
    spatialRDD.buildIndex(IndexType.RTREE, true);
    if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
      List<Point> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
    } else {
      List<Point> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
    }
  }

  /**
   * Test build quadtree index.
   *
   * @throws Exception the exception
   */
  @Test
  public void testBuildQuadtreeIndex() throws Exception {
    PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
    spatialRDD.analyze();
    spatialRDD.spatialPartitioning(gridType);
    spatialRDD.buildIndex(IndexType.QUADTREE, true);
    if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
      List<Point> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
    } else {
      List<Point> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
    }
  }

  @Test
  public void testQuadTreeRTreePartitioningUsesObjectSamples() throws Exception {
    GeometryFactory geometryFactory = new GeometryFactory();
    List<Point> points =
        IntStream.range(0, 128)
            .mapToObj(x -> geometryFactory.createPoint(new Coordinate((double) x, 0.0)))
            .collect(Collectors.toList());
    PointRDD spatialRDD = new PointRDD(sc.parallelize(points, 8));
    spatialRDD.analyze();
    spatialRDD.setNeighborSampleNumber(points.size());
    spatialRDD.setUseKnnSamples(true);
    spatialRDD.setDeduplicateKnnSamples(true);
    spatialRDD.spatialPartitioning(GridType.QUADTREE_RTREE, 16);

    QuadTreeRTPartitioner partitioner = (QuadTreeRTPartitioner) spatialRDD.getPartitioner();
    int gridCount = partitioner.getGrids().size();
    for (List<?> expandedGrids : partitioner.getOverlappedGrids().values()) {
      assertEquals(gridCount, expandedGrids.size());
    }
  }

  @Test
  public void testQuadTreeRTreePartitioningFallsBackForInsufficientUniqueSamples()
      throws Exception {
    GeometryFactory geometryFactory = new GeometryFactory();
    List<Point> points =
        IntStream.range(0, 128)
            .mapToObj(x -> geometryFactory.createPoint(new Coordinate((double) (x / 2), 0.0)))
            .collect(Collectors.toList());
    PointRDD spatialRDD = new PointRDD(sc.parallelize(points, 8));
    spatialRDD.analyze();
    spatialRDD.setNeighborSampleNumber(65);
    spatialRDD.setUseKnnSamples(true);
    spatialRDD.setDeduplicateKnnSamples(true);
    spatialRDD.spatialPartitioning(GridType.QUADTREE_RTREE, 16);

    QuadTreeRTPartitioner partitioner = (QuadTreeRTPartitioner) spatialRDD.getPartitioner();
    int gridCount = partitioner.getGrids().size();
    Point object = geometryFactory.createPoint(new Coordinate(63.0, 0.0));
    assertEquals(
        gridCount,
        partitioner.getSTRForOverlappedGrids().query(object.getEnvelopeInternal()).size());
    for (List<?> expandedGrids : partitioner.getOverlappedGrids().values()) {
      assertEquals(1, expandedGrids.size());
    }
  }

  @Test
  public void testQuadTreeRTreePartitioningFallsBackForInsufficientSamples() throws Exception {
    GeometryFactory geometryFactory = new GeometryFactory();
    List<Point> points =
        IntStream.range(0, 128)
            .mapToObj(x -> geometryFactory.createPoint(new Coordinate((double) x, 0.0)))
            .collect(Collectors.toList());
    PointRDD spatialRDD = new PointRDD(sc.parallelize(points, 8));
    spatialRDD.analyze();
    spatialRDD.setNeighborSampleNumber(points.size() + 1);
    spatialRDD.setUseKnnSamples(true);
    spatialRDD.spatialPartitioning(GridType.QUADTREE_RTREE, 16);

    QuadTreeRTPartitioner partitioner = (QuadTreeRTPartitioner) spatialRDD.getPartitioner();
    int gridCount = partitioner.getGrids().size();
    for (List<?> expandedGrids : partitioner.getOverlappedGrids().values()) {
      assertEquals(1, expandedGrids.size());
    }
    Point object = geometryFactory.createPoint(new Coordinate(63.0, 0.0));
    assertEquals(
        gridCount,
        partitioner.getSTRForOverlappedGrids().query(object.getEnvelopeInternal()).size());
  }
}
