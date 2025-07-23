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

import java.io.Serializable;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.core.rangeJudgement.RangeFilter;
import org.apache.sedona.core.rangeJudgement.RangeFilterUsingIndex;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

// TODO: Auto-generated Javadoc

/** The Class RangeQuery. */
public class RangeQuery implements Serializable {

  /**
   * Spatial range query. Return objects in SpatialRDD are covered/intersected by
   * originalQueryGeometry
   *
   * @param spatialRDD the spatial RDD
   * @param originalQueryGeometry the original query window
   * @param spatialPredicate spatial predicate as query criteria {@code geom <spatialPredicate>
   *     originalQueryGeometry}
   * @param useIndex the use index
   * @return the java RDD
   * @throws Exception the exception
   */
  public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(
      SpatialRDD<T> spatialRDD,
      U originalQueryGeometry,
      SpatialPredicate spatialPredicate,
      boolean useIndex)
      throws Exception {
    U queryGeometry = originalQueryGeometry;
    if (spatialRDD.getCRStransformation()) {
      queryGeometry =
          (U)
              FunctionsGeoTools.transform(
                  originalQueryGeometry,
                  spatialRDD.getSourceEpsgCode(),
                  spatialRDD.getTargetEpgsgCode());
    }

    if (useIndex) {
      if (spatialRDD.indexedRawRDD == null) {
        throw new Exception(
            "[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
      }
      return spatialRDD.indexedRawRDD.mapPartitions(
          new RangeFilterUsingIndex(queryGeometry, spatialPredicate));
    } else {
      return spatialRDD.getRawSpatialRDD().filter(new RangeFilter(queryGeometry, spatialPredicate));
    }
  }

  /**
   * Spatial range query. Return objects in SpatialRDD are covered/intersected by
   * queryWindow/Envelope
   *
   * @param spatialRDD the spatial RDD
   * @param queryWindow the original query window
   * @param spatialPredicate spatial predicate as query criteria {@code geom <spatialPredicate>
   *     queryWindow}
   * @param useIndex the use index
   * @return the java RDD
   * @throws Exception the exception
   */
  public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(
      SpatialRDD<T> spatialRDD,
      Envelope queryWindow,
      SpatialPredicate spatialPredicate,
      boolean useIndex)
      throws Exception {
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
    coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
    coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
    coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
    coordinates[4] = coordinates[0];
    GeometryFactory geometryFactory = new GeometryFactory();
    U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
    return SpatialRangeQuery(spatialRDD, queryGeometry, spatialPredicate, useIndex);
  }

  /**
   * Spatial range query. Return objects in SpatialRDD are covered/intersected by
   * originalQueryGeometry
   *
   * @param spatialRDD the spatial RDD
   * @param originalQueryGeometry the original query window
   * @param considerBoundaryIntersection the consider boundary intersection
   * @param useIndex the use index
   * @return the java RDD
   * @throws Exception the exception
   */
  @Deprecated
  public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(
      SpatialRDD<T> spatialRDD,
      U originalQueryGeometry,
      boolean considerBoundaryIntersection,
      boolean useIndex)
      throws Exception {
    return SpatialRangeQuery(
        spatialRDD,
        originalQueryGeometry,
        considerBoundaryIntersection ? SpatialPredicate.INTERSECTS : SpatialPredicate.COVERED_BY,
        useIndex);
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
  @Deprecated
  public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(
      SpatialRDD<T> spatialRDD,
      Envelope queryWindow,
      boolean considerBoundaryIntersection,
      boolean useIndex)
      throws Exception {
    return SpatialRangeQuery(
        spatialRDD,
        queryWindow,
        considerBoundaryIntersection ? SpatialPredicate.INTERSECTS : SpatialPredicate.COVERED_BY,
        useIndex);
  }

  /**
   * Spatial range query. Return objects in SpatialRDD cover/intersect by queryWindow/Envelope
   *
   * @param spatialRDD the spatial RDD
   * @param queryWindow the original query window
   * @param considerBoundaryIntersection the consider boundary intersection
   * @param useIndex the use index
   * @return the java RDD
   * @throws Exception the exception
   */
  @Deprecated
  public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(
      Envelope queryWindow,
      SpatialRDD<T> spatialRDD,
      boolean considerBoundaryIntersection,
      boolean useIndex)
      throws Exception {
    return SpatialRangeQuery(
        spatialRDD,
        queryWindow,
        considerBoundaryIntersection ? SpatialPredicate.INTERSECTS : SpatialPredicate.COVERS,
        useIndex);
  }

  /**
   * Spatial range query. Return objects in SpatialRDD cover/intersect originalQueryGeometry
   *
   * @param spatialRDD the spatial RDD
   * @param originalQueryGeometry the original query window
   * @param considerBoundaryIntersection the consider boundary intersection
   * @param useIndex the use index
   * @return the java RDD
   * @throws Exception the exception
   */
  @Deprecated
  public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(
      U originalQueryGeometry,
      SpatialRDD<T> spatialRDD,
      boolean considerBoundaryIntersection,
      boolean useIndex)
      throws Exception {
    return SpatialRangeQuery(
        spatialRDD,
        originalQueryGeometry,
        considerBoundaryIntersection ? SpatialPredicate.INTERSECTS : SpatialPredicate.COVERS,
        useIndex);
  }
}
