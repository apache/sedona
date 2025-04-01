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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

public class GenericUniquePartitionerTest {
  private final GeometryFactory factory = new GeometryFactory();

  @Test
  public void testUniquePartition() throws Exception {
    ArrayList<Envelope> grids = new ArrayList<Envelope>();
    grids.add(new Envelope(0, 10, 0, 10));
    grids.add(new Envelope(10, 20, 0, 10));
    grids.add(new Envelope(0, 10, 10, 20));
    grids.add(new Envelope(10, 20, 10, 20));

    FlatGridPartitioner partitioner = new FlatGridPartitioner(grids);
    GenericUniquePartitioner uniquePartitioner = new GenericUniquePartitioner(partitioner);

    assertEquals(partitioner.getGridType(), uniquePartitioner.getGridType());
    assertEquals(partitioner.getGrids(), uniquePartitioner.getGrids());

    Envelope definitelyHasMultiplePartitions = new Envelope(5, 15, 5, 15);

    Iterator<Tuple2<Integer, Geometry>> placedWithDuplicates =
        partitioner.placeObject(factory.toGeometry(definitelyHasMultiplePartitions));
    // Because the geometry is not completely contained by any of the partitions,
    // it also gets placed in the overflow partition (hence 5, not 4)
    assertEquals(5, IteratorUtils.toList(placedWithDuplicates).size());

    Iterator<Tuple2<Integer, Geometry>> placedWithoutDuplicates =
        uniquePartitioner.placeObject(factory.toGeometry(definitelyHasMultiplePartitions));
    assertEquals(1, IteratorUtils.toList(placedWithoutDuplicates).size());
  }
}
