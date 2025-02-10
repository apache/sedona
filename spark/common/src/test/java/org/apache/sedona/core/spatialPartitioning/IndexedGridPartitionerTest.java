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
package org.apache.sedona.core.spatialPartitioning;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

public class IndexedGridPartitionerTest extends TestCase {

  private List<Envelope> getGrids() {
    List<Envelope> grids = new ArrayList<>();
    grids.add(new Envelope(0, 50, 0, 50));
    grids.add(new Envelope(50, 100, 0, 50));
    grids.add(new Envelope(0, 50, 50, 100));
    grids.add(new Envelope(50, 100, 50, 100));
    return grids;
  }

  private IndexedGridPartitioner getPartitioner(Boolean preserveUncontainedGeometries) {
    return new IndexedGridPartitioner(getGrids(), preserveUncontainedGeometries);
  }

  public void testPlaceObjectPreserveContainedGeometries() throws Exception {
    IndexedGridPartitioner partitioner = getPartitioner(true);
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry spatialObject = geometryFactory.createPoint(new Coordinate(25, 25));
    Iterator<Tuple2<Integer, Geometry>> result = partitioner.placeObject(spatialObject);

    List<Tuple2<Integer, Geometry>> resultList = new ArrayList<>();
    result.forEachRemaining(resultList::add);

    Assert.assertFalse(resultList.isEmpty());
    Assert.assertEquals(1, resultList.size());
    Assert.assertEquals(0, (int) resultList.get(0)._1());
  }

  public void testPlaceObjectDoesntPreserveUncontainedGeometries() throws Exception {
    IndexedGridPartitioner partitioner = getPartitioner(false);
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry spatialObject = geometryFactory.createPoint(new Coordinate(-25, -25));
    Iterator<Tuple2<Integer, Geometry>> result = partitioner.placeObject(spatialObject);
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void testGetGrids() {
    IndexedGridPartitioner partitioner = getPartitioner(true);
    Assert.assertEquals(getGrids(), partitioner.getGrids());
  }

  @Test
  public void testNumPartitions() {
    IndexedGridPartitioner partitioner = getPartitioner(true);
    Assert.assertEquals(5, partitioner.numPartitions());

    partitioner = getPartitioner(false);
    Assert.assertEquals(4, partitioner.numPartitions());
  }

  @Test
  public void testEquals() {
    IndexedGridPartitioner partitioner = getPartitioner(true);
    List<Envelope> grids = new ArrayList<>();
    grids.add(new Envelope(0, 50, 0, 50));
    grids.add(new Envelope(50, 100, 0, 50));
    grids.add(new Envelope(0, 50, 50, 100));
    grids.add(new Envelope(50, 100, 50, 100));
    IndexedGridPartitioner otherPartitioner = new IndexedGridPartitioner(grids, true);
    Assert.assertTrue(partitioner.equals(otherPartitioner));
  }
}
