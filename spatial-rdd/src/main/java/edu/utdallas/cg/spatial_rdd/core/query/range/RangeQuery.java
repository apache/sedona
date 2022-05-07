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

package edu.utdallas.cg.spatial_rdd.core.query.range;

import edu.utdallas.cg.spatial_rdd.core.query.range.filter.impl.RangeFilter;
import edu.utdallas.cg.spatial_rdd.core.query.range.filter.impl.RangeFilterUsingIndex;
import edu.utdallas.cg.spatial_rdd.core.rdd.SpatialRDD;

import java.io.Serializable;

import lombok.SneakyThrows;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.SpatialIndex;

public class RangeQuery implements Serializable {

  public static <U extends Geometry, T extends Geometry> JavaRDD<T> spatialRangeQuery(
      SpatialRDD<T> spatialRDD,
      U originalQueryGeometry,
      boolean considerBoundaryIntersection,
      boolean useIndex)
      throws Exception {

    if (useIndex) {
      JavaRDD<SpatialIndex> spatialIndexedRDD =
          spatialRDD.indexedRawRDD == null ? spatialRDD.indexedRDD : spatialRDD.indexedRawRDD;

      if (spatialIndexedRDD == null) {
        throw new Exception(
            "[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD or spatialPartitionedRDD.");
      }
      return spatialIndexedRDD.mapPartitions(
          new RangeFilterUsingIndex(originalQueryGeometry, considerBoundaryIntersection, true));
    } else {
      return spatialRDD
          .getRawRdd()
          .filter(new RangeFilter(originalQueryGeometry, considerBoundaryIntersection, true));
    }
  }

  /**
   * Spatial range query. Return objects in SpatialRDD are covered/intersected by
   * queryWindow/Envelope
   *
   * @param spatialRDD the spatial RDD
   * @param queryWindow the original query window
   * @param considerBoundaryIntersection the consider boundary intersection
   * @param useIndex the use index
   * @return the java RDD
   * @throws Exception the exception
   */
  @SneakyThrows
  public static <U extends Geometry, T extends Geometry> JavaRDD<T> spatialRangeQuery(
      SpatialRDD<T> spatialRDD,
      Envelope queryWindow,
      boolean considerBoundaryIntersection,
      boolean useIndex) {
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
    coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
    coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
    coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
    coordinates[4] = coordinates[0];
    GeometryFactory geometryFactory = new GeometryFactory();
    U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
    return spatialRangeQuery(spatialRDD, queryGeometry, considerBoundaryIntersection, useIndex);
  }
}
