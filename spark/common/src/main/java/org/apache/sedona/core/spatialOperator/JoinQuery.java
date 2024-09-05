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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.core.enums.DistanceMetric;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.joinJudgement.*;
import org.apache.sedona.core.monitoring.Metrics;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRDD.CircleRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

public class JoinQuery {
  private static final Logger log = LogManager.getLogger(JoinQuery.class);

  private static <U extends Geometry, T extends Geometry> void verifyCRSMatch(
      SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD) throws Exception {
    // Check CRS information before doing calculation. The two input RDDs are supposed to have the
    // same EPSG code if they require CRS transformation.
    if (spatialRDD.getCRStransformation() != queryRDD.getCRStransformation()) {
      throw new IllegalArgumentException(
          "[JoinQuery] input RDD doesn't perform necessary CRS transformation. Please check your RDD constructors.");
    }

    if (spatialRDD.getCRStransformation() && queryRDD.getCRStransformation()) {
      if (!spatialRDD.getTargetEpgsgCode().equalsIgnoreCase(queryRDD.getTargetEpgsgCode())) {
        throw new IllegalArgumentException(
            "[JoinQuery] the EPSG codes of two input RDDs are different. Please check your RDD constructors.");
      }
    }
  }

  private static <U extends Geometry, T extends Geometry> void verifyPartitioningMatch(
      SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD) throws Exception {
    Objects.requireNonNull(
        spatialRDD.spatialPartitionedRDD,
        "[JoinQuery] spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
    Objects.requireNonNull(
        queryRDD.spatialPartitionedRDD,
        "[JoinQuery] queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");

    final SpatialPartitioner spatialPartitioner = spatialRDD.getPartitioner();
    final SpatialPartitioner queryPartitioner = queryRDD.getPartitioner();

    if (!queryPartitioner.equals(spatialPartitioner)) {
      throw new IllegalArgumentException(
          "[JoinQuery] queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
    }

    final int spatialNumPart = spatialRDD.spatialPartitionedRDD.getNumPartitions();
    final int queryNumPart = queryRDD.spatialPartitionedRDD.getNumPartitions();
    if (spatialNumPart != queryNumPart) {
      throw new IllegalArgumentException(
          "[JoinQuery] numbers of partitions in queryRDD and spatialRDD don't match: "
              + queryNumPart
              + " vs. "
              + spatialNumPart
              + ". Please make sure they both use the same partitioning otherwise wrong results will appear.");
    }
  }

  private static <U extends Geometry, T extends Geometry>
      JavaPairRDD<U, List<T>> collectGeometriesByKey(JavaPairRDD<U, T> input) {
    return input
        .groupBy(t -> GeomUtils.hashCode(t._1))
        .values()
        .<U, List<T>>mapToPair(
            t -> {
              List<T> values = new ArrayList<T>();
              Iterator<Tuple2<U, T>> it = t.iterator();
              Tuple2<U, T> firstTpl = it.next();
              U key = firstTpl._1;
              values.add(firstTpl._2);
              while (it.hasNext()) {
                values.add(it.next()._2);
              }
              return new Tuple2<U, List<T>>(key, values);
            });
  }

  private static <U extends Geometry, T extends Geometry> JavaPairRDD<U, Long> countGeometriesByKey(
      JavaPairRDD<U, T> input) {
    return input.aggregateByKey(
        0L,
        new Function2<Long, T, Long>() {

          @Override
          public Long call(Long count, T t) throws Exception {
            return count + 1;
          }
        },
        new Function2<Long, Long, Long>() {

          @Override
          public Long call(Long count1, Long count2) throws Exception {
            return count1 + count2;
          }
        });
  }

  /**
   * Inner joins two sets of geometries on specified spatial predicate.
   *
   * <p>{@code spatialPredicate} is the spatial predicate in join condition {@code spatialRDD
   * <spatialPredicate> queryRDD}
   *
   * <p>If {@code useIndex} is false, the join uses nested loop algorithm to identify matching
   * geometries.
   *
   * <p>If {@code useIndex} is true, the join scans query windows and uses an index of geometries
   * built prior to invoking the join to lookup matches.
   *
   * <p>Duplicate geometries present in the input queryWindowRDD, regardless of their non-spatial
   * attributes, will not be reflected in the join results. Duplicate geometries present in the
   * input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join
   * results.
   *
   * @param <U> Type of the geometries in queryWindowRDD set
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries which serve as query windows
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param spatialPredicate Spatial predicate in join condition {@code spatialRDD
   *     <spatialPredicate> queryRDD}
   * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
   * @throws Exception the exception
   */
  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> SpatialJoinQuery(
      SpatialRDD<T> spatialRDD,
      SpatialRDD<U> queryRDD,
      boolean useIndex,
      SpatialPredicate spatialPredicate)
      throws Exception {
    final JoinParams joinParams =
        new JoinParams(useIndex, SpatialPredicate.inverse(spatialPredicate));
    final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
    return collectGeometriesByKey(joinResults);
  }

  /**
   * Inner joins two sets of geometries on 'covers' or 'intersects' relationship.
   *
   * <p>If {@code considerBoundaryIntersection} is {@code true}, returns pairs of geometries which
   * intersect. Otherwise, returns pairs of geometries where first geometry covers second geometry.
   *
   * <p>If {@code useIndex} is false, the join uses nested loop algorithm to identify matching
   * geometries.
   *
   * <p>If {@code useIndex} is true, the join scans query windows and uses an index of geometries
   * built prior to invoking the join to lookup matches.
   *
   * <p>Duplicate geometries present in the input queryWindowRDD, regardless of their non-spatial
   * attributes, will not be reflected in the join results. Duplicate geometries present in the
   * input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join
   * results.
   *
   * @param <U> Type of the geometries in queryWindowRDD set
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries which serve as query windows
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'covers'
   *     otherwise
   * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
   * @throws Exception the exception
   */
  @Deprecated
  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> SpatialJoinQuery(
      SpatialRDD<T> spatialRDD,
      SpatialRDD<U> queryRDD,
      boolean useIndex,
      boolean considerBoundaryIntersection)
      throws Exception {
    final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
    final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
    return collectGeometriesByKey(joinResults);
  }

  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> SpatialJoinQuery(
      SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams) throws Exception {
    final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
    return collectGeometriesByKey(joinResults);
  }

  /**
   * Inner joins two sets of geometries on specified spatial predicate. Results are put in a flat
   * pair format.
   *
   * <p>{@code spatialPredicate} is the spatial predicate in join condition {@code spatialRDD
   * <spatialPredicate> queryRDD}.
   *
   * <p>If {@code useIndex} is false, the join uses nested loop algorithm to identify matching
   * geometries.
   *
   * <p>If {@code useIndex} is true, the join scans query windows and uses an index of geometries
   * built prior to invoking the join to lookup matches.
   *
   * <p>Duplicates present in the input RDDs will be reflected in the join results.
   *
   * @param <U> Type of the geometries in queryWindowRDD set
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries which serve as query windows
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param spatialPredicate Spatial predicate in join condition {@code spatialRDD
   *     <spatialPredicate> queryRDD}
   * @return RDD of pairs of matching geometries
   * @throws Exception the exception
   */
  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, T> SpatialJoinQueryFlat(
      SpatialRDD<T> spatialRDD,
      SpatialRDD<U> queryRDD,
      boolean useIndex,
      SpatialPredicate spatialPredicate)
      throws Exception {
    final JoinParams params = new JoinParams(useIndex, SpatialPredicate.inverse(spatialPredicate));
    return spatialJoin(queryRDD, spatialRDD, params);
  }

  /**
   * Inner joins two sets of geometries on 'covers' or 'intersects' relationship. Results are put in
   * a flat pair format.
   *
   * <p>If {@code considerBoundaryIntersection} is {@code true}, returns pairs of geometries which
   * intersect. Otherwise, returns pairs of geometries where first geometry covers second geometry.
   *
   * <p>If {@code useIndex} is false, the join uses nested loop algorithm to identify matching
   * geometries.
   *
   * <p>If {@code useIndex} is true, the join scans query windows and uses an index of geometries
   * built prior to invoking the join to lookup matches.
   *
   * <p>Duplicates present in the input RDDs will be reflected in the join results.
   *
   * @param <U> Type of the geometries in queryWindowRDD set
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries which serve as query windows
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'covers'
   *     otherwise
   * @return RDD of pairs of matching geometries
   * @throws Exception the exception
   */
  @Deprecated
  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, T> SpatialJoinQueryFlat(
      SpatialRDD<T> spatialRDD,
      SpatialRDD<U> queryRDD,
      boolean useIndex,
      boolean considerBoundaryIntersection)
      throws Exception {
    final JoinParams params = new JoinParams(useIndex, considerBoundaryIntersection);
    return spatialJoin(queryRDD, spatialRDD, params);
  }

  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, T> SpatialJoinQueryFlat(
      SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams) throws Exception {
    return spatialJoin(queryRDD, spatialRDD, joinParams);
  }

  /**
   * {@link #SpatialJoinQueryFlat(SpatialRDD, SpatialRDD, boolean, SpatialPredicate)} count by key.
   *
   * <p>Duplicate geometries present in the input queryWindowRDD RDD, regardless of their
   * non-spatial attributes, will not be reflected in the join results. Duplicate geometries present
   * in the input spatialRDD RDD, regardless of their non-spatial attributes, will be reflected in
   * the join results.
   *
   * @param <U> Type of the geometries in queryWindowRDD set
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries which serve as query windows
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param spatialPredicate Spatial predicate in join condition {@code spatialRDD
   *     <spatialPredicate> queryRDD}
   * @return the result of {@link #SpatialJoinQueryFlat(SpatialRDD, SpatialRDD, boolean,
   *     SpatialPredicate)}, but in this pair RDD, each pair contains a geometry and the count of
   *     matching geometries
   * @throws Exception the exception
   */
  public static <U extends Geometry, T extends Geometry>
      JavaPairRDD<U, Long> SpatialJoinQueryCountByKey(
          SpatialRDD<T> spatialRDD,
          SpatialRDD<U> queryRDD,
          boolean useIndex,
          SpatialPredicate spatialPredicate)
          throws Exception {
    final JoinParams joinParams =
        new JoinParams(useIndex, SpatialPredicate.inverse(spatialPredicate));
    final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
    return countGeometriesByKey(joinResults);
  }

  /**
   * {@link #SpatialJoinQueryFlat(SpatialRDD, SpatialRDD, boolean, boolean)} count by key.
   *
   * <p>Duplicate geometries present in the input queryWindowRDD RDD, regardless of their
   * non-spatial attributes, will not be reflected in the join results. Duplicate geometries present
   * in the input spatialRDD RDD, regardless of their non-spatial attributes, will be reflected in
   * the join results.
   *
   * @param <U> Type of the geometries in queryWindowRDD set
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries which serve as query windows
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'covers'
   *     otherwise
   * @return the result of {@link #SpatialJoinQueryFlat(SpatialRDD, SpatialRDD, boolean, boolean)},
   *     but in this pair RDD, each pair contains a geometry and the count of matching geometries
   * @throws Exception the exception
   */
  @Deprecated
  public static <U extends Geometry, T extends Geometry>
      JavaPairRDD<U, Long> SpatialJoinQueryCountByKey(
          SpatialRDD<T> spatialRDD,
          SpatialRDD<U> queryRDD,
          boolean useIndex,
          boolean considerBoundaryIntersection)
          throws Exception {
    final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
    final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
    return countGeometriesByKey(joinResults);
  }

  public static <U extends Geometry, T extends Geometry>
      JavaPairRDD<U, Long> SpatialJoinQueryCountByKey(
          SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams)
          throws Exception {
    final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
    return countGeometriesByKey(joinResults);
  }

  /**
   * Joins two sets of geometries on specified distance metric and finds the k nearest neighbors.
   *
   * <p>Duplicate geometries present in the input queryWindowRDD, regardless of their non-spatial
   * attributes, will not be reflected in the join results. Duplicate geometries present in the
   * input objectRDD, regardless of their non-spatial attributes, will be reflected in the join
   * results.
   *
   * @param <U> Type of the geometries in queryWindowRDD set
   * @param <T> Type of the geometries in objectRDD set
   * @param objectRDD {@code objectRDD} is the set of geometries (neighbors) to be queried
   * @param queryRDD {@code queryRDD} is the set of geometries which serve as query geometries
   *     (center points)
   * @param indexType {@code indexType} is the index type to use for the join
   * @param k {@code k} is the number of nearest neighbors to find
   * @param distanceMetric {@code distanceMetric} is the distance metric to use
   * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
   * @throws Exception the exception
   */
  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> KNNJoinQuery(
      SpatialRDD<T> objectRDD,
      SpatialRDD<U> queryRDD,
      IndexType indexType,
      int k,
      DistanceMetric distanceMetric)
      throws Exception {
    final JoinParams joinParams =
        new JoinParams(true, null, IndexType.RTREE, null, k, distanceMetric, null);

    final JavaPairRDD<U, T> joinResults = knnJoin(queryRDD, objectRDD, joinParams, false, false);
    return collectGeometriesByKey(joinResults);
  }

  /**
   * Inner joins two sets of geometries, where the query windows are circles (aka. distance join).
   * Results are put in a flat pair format.
   *
   * <p>{@code spatialPredicate} is the spatial predicate in join condition {@code spatialRDD
   * <spatialPredicate> queryRDD}.
   *
   * <p>If {@code useIndex} is false, the join uses nested loop algorithm to identify matching
   * circle/geometry.
   *
   * <p>If {@code useIndex} is true, the join scans circles and uses an index of geometries built
   * prior to invoking the join to lookup matches.
   *
   * <p>Duplicates present in the input RDDs will be reflected in the join results.
   *
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param spatialPredicate Spatial predicate in join condition {@code spatialRDD
   *     <spatialPredicate> queryRDD}, should be one of {@code INTERSECTS} and {@code COVERED_BY}
   * @return RDD of pairs of matching geometries
   * @throws Exception the exception
   */
  public static <T extends Geometry> JavaPairRDD<Geometry, T> DistanceJoinQueryFlat(
      SpatialRDD<T> spatialRDD,
      CircleRDD queryRDD,
      boolean useIndex,
      SpatialPredicate spatialPredicate)
      throws Exception {
    if (spatialPredicate != SpatialPredicate.COVERED_BY
        && spatialPredicate != SpatialPredicate.INTERSECTS) {
      throw new IllegalArgumentException(
          "Spatial predicate for distance join should be one of INTERSECTS and COVERED_BY");
    }
    final JoinParams joinParams =
        new JoinParams(useIndex, SpatialPredicate.inverse(spatialPredicate));
    return distanceJoin(spatialRDD, queryRDD, joinParams);
  }

  /**
   * Inner joins two sets of geometries on 'coveredBy' relationship (aka. distance join). Results
   * are put in a flat pair format.
   *
   * <p>If {@code considerBoundaryIntersection} is {@code true}, returns pairs of circle/geometry
   * which intersect. Otherwise, returns pairs of geometries where first circle covers second
   * geometry.
   *
   * <p>If {@code useIndex} is false, the join uses nested loop algorithm to identify matching
   * circle/geometry.
   *
   * <p>If {@code useIndex} is true, the join scans circles and uses an index of geometries built
   * prior to invoking the join to lookup matches.
   *
   * <p>Duplicates present in the input RDDs will be reflected in the join results.
   *
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param considerBoundaryIntersection consider boundary intersection
   * @return RDD of pairs of matching geometries
   * @throws Exception the exception
   */
  @Deprecated
  public static <T extends Geometry> JavaPairRDD<Geometry, T> DistanceJoinQueryFlat(
      SpatialRDD<T> spatialRDD,
      CircleRDD queryRDD,
      boolean useIndex,
      boolean considerBoundaryIntersection)
      throws Exception {
    final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
    return distanceJoin(spatialRDD, queryRDD, joinParams);
  }

  public static <T extends Geometry> JavaPairRDD<Geometry, T> DistanceJoinQueryFlat(
      SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams) throws Exception {
    return distanceJoin(spatialRDD, queryRDD, joinParams);
  }

  /**
   * Inner joins two sets of geometries, where the query windows are circles (aka. distance join).
   * The query window objects are converted to circle objects. The radius is the given distance.
   * Eventually, the original window objects are recovered and outputted.
   *
   * <p>{@code spatialPredicate} is the spatial predicate in join condition {@code spatialRDD
   * <spatialPredicate> queryRDD}.
   *
   * <p>If {@code useIndex} is false, the join uses nested loop algorithm to identify matching
   * circle/geometry.
   *
   * <p>If {@code useIndex} is true, the join scans circles and uses an index of geometries built
   * prior to invoking the join to lookup matches.
   *
   * <p>Duplicate geometries present in the input CircleRDD, regardless of their non-spatial
   * attributes, will not be reflected in the join results. Duplicate geometries present in the
   * input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join
   * results.
   *
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param spatialPredicate Spatial predicate in join condition {@code spatialRDD
   *     <spatialPredicate> queryRDD}, should be one of {@code INTERSECTS} and {@code COVERED_BY}
   * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
   * @throws Exception the exception
   */
  public static <T extends Geometry> JavaPairRDD<Geometry, List<T>> DistanceJoinQuery(
      SpatialRDD<T> spatialRDD,
      CircleRDD queryRDD,
      boolean useIndex,
      SpatialPredicate spatialPredicate)
      throws Exception {
    if (spatialPredicate != SpatialPredicate.COVERED_BY
        && spatialPredicate != SpatialPredicate.INTERSECTS) {
      throw new IllegalArgumentException(
          "Spatial predicate for distance join should be one of INTERSECTS and COVERED_BY");
    }
    final JoinParams joinParams =
        new JoinParams(useIndex, SpatialPredicate.inverse(spatialPredicate));
    JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
    return collectGeometriesByKey(joinResults);
  }

  /**
   * Inner joins two sets of geometries on 'coveredBy' relationship (aka. distance join). The query
   * window objects are converted to circle objects. The radius is the given distance. Eventually,
   * the original window objects are recovered and outputted.
   *
   * <p>If {@code considerBoundaryIntersection} is {@code true}, returns pairs of circle/geometry
   * which intersect. Otherwise, returns pairs of geometries where first circle covers second
   * geometry.
   *
   * <p>If {@code useIndex} is false, the join uses nested loop algorithm to identify matching
   * circle/geometry.
   *
   * <p>If {@code useIndex} is true, the join scans circles and uses an index of geometries built
   * prior to invoking the join to lookup matches.
   *
   * <p>Duplicate geometries present in the input CircleRDD, regardless of their non-spatial
   * attributes, will not be reflected in the join results. Duplicate geometries present in the
   * input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join
   * results.
   *
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param considerBoundaryIntersection consider boundary intersection
   * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
   * @throws Exception the exception
   */
  @Deprecated
  public static <T extends Geometry> JavaPairRDD<Geometry, List<T>> DistanceJoinQuery(
      SpatialRDD<T> spatialRDD,
      CircleRDD queryRDD,
      boolean useIndex,
      boolean considerBoundaryIntersection)
      throws Exception {
    final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
    JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
    return collectGeometriesByKey(joinResults);
  }

  public static <T extends Geometry> JavaPairRDD<Geometry, List<T>> DistanceJoinQuery(
      SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams) throws Exception {
    JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
    return collectGeometriesByKey(joinResults);
  }

  /**
   * {@link #DistanceJoinQueryFlat(SpatialRDD, CircleRDD, boolean, boolean)} count by key.
   *
   * <p>Duplicate geometries present in the input CircleRDD, regardless of their non-spatial
   * attributes, will not be reflected in the join results. Duplicate geometries present in the
   * input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join
   * results.
   *
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param spatialPredicate Spatial predicate in join condition {@code spatialRDD
   *     <spatialPredicate> queryRDD}, should be one of {@code INTERSECTS} and {@code COVERED_BY}
   * @return the result of {@link #DistanceJoinQueryFlat(SpatialRDD, CircleRDD, boolean, boolean)},
   *     but in this pair RDD, each pair contains a geometry and the count of matching geometries
   * @throws Exception the exception
   */
  public static <T extends Geometry> JavaPairRDD<Geometry, Long> DistanceJoinQueryCountByKey(
      SpatialRDD<T> spatialRDD,
      CircleRDD queryRDD,
      boolean useIndex,
      SpatialPredicate spatialPredicate)
      throws Exception {
    if (spatialPredicate != SpatialPredicate.COVERED_BY
        && spatialPredicate != SpatialPredicate.INTERSECTS) {
      throw new IllegalArgumentException(
          "Spatial predicate for distance join should be one of INTERSECTS and COVERED_BY");
    }
    final JoinParams joinParams =
        new JoinParams(useIndex, SpatialPredicate.inverse(spatialPredicate));
    final JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
    return countGeometriesByKey(joinResults);
  }

  /**
   * {@link #DistanceJoinQueryFlat(SpatialRDD, CircleRDD, boolean, boolean)} count by key.
   *
   * <p>Duplicate geometries present in the input CircleRDD, regardless of their non-spatial
   * attributes, will not be reflected in the join results. Duplicate geometries present in the
   * input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join
   * results.
   *
   * @param <T> Type of the geometries in spatialRDD set
   * @param spatialRDD Set of geometries
   * @param queryRDD Set of geometries
   * @param useIndex Boolean indicating whether the join should use the index from {@code
   *     spatialRDD.indexedRDD}
   * @param considerBoundaryIntersection consider boundary intersection
   * @return the result of {@link #DistanceJoinQueryFlat(SpatialRDD, CircleRDD, boolean, boolean)},
   *     but in this pair RDD, each pair contains a geometry and the count of matching geometries
   * @throws Exception the exception
   */
  @Deprecated
  public static <T extends Geometry> JavaPairRDD<Geometry, Long> DistanceJoinQueryCountByKey(
      SpatialRDD<T> spatialRDD,
      CircleRDD queryRDD,
      boolean useIndex,
      boolean considerBoundaryIntersection)
      throws Exception {
    final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
    final JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
    return countGeometriesByKey(joinResults);
  }

  public static <T extends Geometry> JavaPairRDD<Geometry, Long> DistanceJoinQueryCountByKey(
      SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams) throws Exception {
    final JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
    return countGeometriesByKey(joinResults);
  }

  /**
   * Note: INTERNAL FUNCTION. API COMPATIBILITY IS NOT GUARANTEED. DO NOT USE IF YOU DON'T KNOW WHAT
   * IT IS.
   */
  public static <T extends Geometry> JavaPairRDD<Geometry, T> distanceJoin(
      SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams) throws Exception {
    JavaPairRDD<Circle, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
    return joinResults.mapToPair(
        new PairFunction<Tuple2<Circle, T>, Geometry, T>() {
          @Override
          public Tuple2<Geometry, T> call(Tuple2<Circle, T> circleTTuple2) throws Exception {
            return new Tuple2<Geometry, T>(
                circleTTuple2._1().getCenterGeometry(), circleTTuple2._2());
          }
        });
  }

  /**
   * Note: INTERNAL FUNCTION. API COMPATIBILITY IS NOT GUARANTEED. DO NOT USE IF YOU DON'T KNOW WHAT
   * IT IS.
   */
  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, T> spatialJoin(
      SpatialRDD<U> leftRDD, SpatialRDD<T> rightRDD, JoinParams joinParams) throws Exception {

    verifyCRSMatch(leftRDD, rightRDD);
    verifyPartitioningMatch(leftRDD, rightRDD);

    SparkContext sparkContext = leftRDD.spatialPartitionedRDD.context();
    LongAccumulator buildCount = Metrics.createMetric(sparkContext, "buildCount");
    LongAccumulator streamCount = Metrics.createMetric(sparkContext, "streamCount");
    LongAccumulator resultCount = Metrics.createMetric(sparkContext, "resultCount");
    LongAccumulator candidateCount = Metrics.createMetric(sparkContext, "candidateCount");

    final SpatialPartitioner partitioner =
        (SpatialPartitioner) rightRDD.spatialPartitionedRDD.partitioner().get();
    final DedupParams dedupParams = partitioner.getDedupParams();
    final SparkContext cxt = leftRDD.rawSpatialRDD.context();

    final JavaRDD<Pair<U, T>> joinResult;
    if (joinParams.useIndex) {
      if (rightRDD.indexedRDD != null) {
        final RightIndexLookupJudgement judgement =
            new RightIndexLookupJudgement(
                joinParams.spatialPredicate, buildCount, streamCount, resultCount, candidateCount);
        joinResult = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement);
      } else if (leftRDD.indexedRDD != null) {
        final LeftIndexLookupJudgement judgement =
            new LeftIndexLookupJudgement(
                joinParams.spatialPredicate, buildCount, streamCount, resultCount, candidateCount);
        joinResult = leftRDD.indexedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
      } else {
        log.warn("UseIndex is true, but no index exists. Will build index on the fly.");
        DynamicIndexLookupJudgement judgement =
            new DynamicIndexLookupJudgement(
                joinParams.spatialPredicate,
                joinParams.indexType,
                joinParams.joinBuildSide,
                buildCount,
                streamCount,
                resultCount,
                candidateCount);
        joinResult =
            leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
      }
    } else {
      NestedLoopJudgement judgement =
          new NestedLoopJudgement(
              joinParams.spatialPredicate, buildCount, streamCount, resultCount, candidateCount);
      joinResult =
          rightRDD.spatialPartitionedRDD.zipPartitions(leftRDD.spatialPartitionedRDD, judgement);
    }

    return joinResult
        .mapPartitionsWithIndex(
            new DuplicatesFilter(new JavaSparkContext(cxt).broadcast(dedupParams)), false)
        .mapToPair(
            (PairFunction<Pair<U, T>, U, T>) pair -> new Tuple2<>(pair.getKey(), pair.getValue()));
  }

  private static <U extends Geometry, T extends Geometry> void verifyPartitioningNumberMatch(
      SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD) throws Exception {
    Objects.requireNonNull(
        spatialRDD.spatialPartitionedRDD,
        "[JoinQuery] spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
    Objects.requireNonNull(
        queryRDD.spatialPartitionedRDD,
        "[JoinQuery] queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");

    final SpatialPartitioner spatialPartitioner = spatialRDD.getPartitioner();
    final SpatialPartitioner queryPartitioner = queryRDD.getPartitioner();

    final int spatialNumPart = spatialRDD.spatialPartitionedRDD.getNumPartitions();
    final int queryNumPart = queryRDD.spatialPartitionedRDD.getNumPartitions();
    if (spatialNumPart != queryNumPart) {
      throw new IllegalArgumentException(
          "[JoinQuery] numbers of partitions in queryRDD and spatialRDD don't match: "
              + queryNumPart
              + " vs. "
              + spatialNumPart
              + ". Please make sure they both use the same partitioning otherwise wrong results will appear.");
    }
  }

  /**
   * @param queryRDD {@code queryRDD} is the set of geometries which serve as query geometries
   *     (center points)
   * @param objectRDD {@code objectRDD} is the set of geometries (neighbors) to be queried
   * @param joinParams {@code joinParams} is the parameters for the join
   * @param includeTies {@code includeTies} is a boolean indicating whether to include ties
   * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
   * @param <U> Type of the geometries in queryRDD set
   * @param <T> Type of the geometries in objectRDD set
   * @throws Exception
   */
  public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, T> knnJoin(
      SpatialRDD<U> queryRDD,
      SpatialRDD<T> objectRDD,
      JoinParams joinParams,
      boolean includeTies,
      boolean broadcastJoin)
      throws Exception {
    verifyCRSMatch(queryRDD, objectRDD);
    if (!broadcastJoin) verifyPartitioningNumberMatch(queryRDD, objectRDD);

    SparkContext sparkContext = queryRDD.rawSpatialRDD.context();
    LongAccumulator buildCount = Metrics.createMetric(sparkContext, "buildCount");
    LongAccumulator streamCount = Metrics.createMetric(sparkContext, "streamCount");
    LongAccumulator resultCount = Metrics.createMetric(sparkContext, "resultCount");
    LongAccumulator candidateCount = Metrics.createMetric(sparkContext, "candidateCount");

    final Broadcast<STRtree> broadcastedTreeIndex;
    if (broadcastJoin) {
      // adjust auto broadcast threshold to avoid building index on large RDDs
      STRtree strTree = objectRDD.coalesceAndBuildRawIndex(IndexType.RTREE);
      broadcastedTreeIndex = JavaSparkContext.fromSparkContext(sparkContext).broadcast(strTree);
    } else {
      broadcastedTreeIndex = null;
    }

    // The reason for using objectRDD as the right side is that the partitions are built on the
    // right side.
    final JavaRDD<Pair<U, T>> joinResult;
    if (objectRDD.indexedRDD != null) {
      final KnnJoinIndexJudgement judgement =
          new KnnJoinIndexJudgement(
              joinParams.k,
              joinParams.distanceMetric,
              includeTies,
              broadcastedTreeIndex,
              buildCount,
              streamCount,
              resultCount,
              candidateCount);
      joinResult = queryRDD.spatialPartitionedRDD.zipPartitions(objectRDD.indexedRDD, judgement);
    } else if (broadcastedTreeIndex != null) {
      final KnnJoinIndexJudgement judgement =
          new KnnJoinIndexJudgement(
              joinParams.k,
              joinParams.distanceMetric,
              includeTies,
              broadcastedTreeIndex,
              buildCount,
              streamCount,
              resultCount,
              candidateCount);
      int numPartitionsObjects = objectRDD.rawSpatialRDD.getNumPartitions();
      joinResult =
          queryRDD
              .rawSpatialRDD
              .repartition(numPartitionsObjects)
              .zipPartitions(objectRDD.rawSpatialRDD, judgement);
    } else {
      throw new IllegalArgumentException("No index found on the input RDDs.");
    }

    return joinResult.mapToPair(
        (PairFunction<Pair<U, T>, U, T>) pair -> new Tuple2<>(pair.getKey(), pair.getValue()));
  }

  public static final class JoinParams {
    public final boolean useIndex;
    public final SpatialPredicate spatialPredicate;
    public final IndexType indexType;
    public final JoinBuildSide joinBuildSide;

    // KNN specific parameters
    public final int k;
    public final DistanceMetric distanceMetric;
    public final Double searchRadius;

    public JoinParams(
        boolean useIndex,
        SpatialPredicate spatialPredicate,
        IndexType polygonIndexType,
        JoinBuildSide joinBuildSide) {
      this(useIndex, spatialPredicate, polygonIndexType, joinBuildSide, -1, null, null);
    }

    public JoinParams(
        boolean useIndex,
        SpatialPredicate spatialPredicate,
        IndexType polygonIndexType,
        JoinBuildSide joinBuildSide,
        int k,
        DistanceMetric distanceMetric,
        Double searchRadius) {
      this.useIndex = useIndex;
      this.spatialPredicate = spatialPredicate;
      this.indexType = polygonIndexType;
      this.joinBuildSide = joinBuildSide;
      this.k = k;
      this.distanceMetric = distanceMetric;
      this.searchRadius = searchRadius;
    }

    public JoinParams(boolean useIndex, SpatialPredicate spatialPredicate) {
      this(useIndex, spatialPredicate, IndexType.RTREE, JoinBuildSide.RIGHT);
    }

    @Deprecated
    public JoinParams(boolean useIndex, boolean considerBoundaryIntersection) {
      this(useIndex, considerBoundaryIntersection, IndexType.RTREE, JoinBuildSide.RIGHT);
    }

    @Deprecated
    public JoinParams(
        boolean useIndex,
        boolean considerBoundaryIntersection,
        IndexType polygonIndexType,
        JoinBuildSide joinBuildSide) {
      this(
          useIndex,
          considerBoundaryIntersection ? SpatialPredicate.INTERSECTS : SpatialPredicate.COVERS,
          polygonIndexType,
          joinBuildSide);
    }
  }
}
