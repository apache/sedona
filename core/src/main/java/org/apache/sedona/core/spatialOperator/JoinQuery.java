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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.joinJudgement.DynamicIndexLookupJudgement;
import org.apache.sedona.core.joinJudgement.LeftIndexLookupJudgement;
import org.apache.sedona.core.joinJudgement.NestedLoopJudgement;
import org.apache.sedona.core.joinJudgement.RightIndexLookupJudgement;
import org.apache.sedona.core.monitoring.Metric;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRDD.CircleRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.utils.GeomUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sedona.core.monitoring.Metrics;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class JoinQuery
{
    private static final Logger log = LogManager.getLogger(JoinQuery.class);

    private static <U extends Geometry, T extends Geometry> void verifyCRSMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD)
            throws Exception
    {
        // Check CRS information before doing calculation. The two input RDDs are supposed to have the same EPSG code if they require CRS transformation.
        if (spatialRDD.getCRStransformation() != queryRDD.getCRStransformation()) {
            throw new IllegalArgumentException("[JoinQuery] input RDD doesn't perform necessary CRS transformation. Please check your RDD constructors.");
        }

        if (spatialRDD.getCRStransformation() && queryRDD.getCRStransformation()) {
            if (!spatialRDD.getTargetEpgsgCode().equalsIgnoreCase(queryRDD.getTargetEpgsgCode())) {
                throw new IllegalArgumentException("[JoinQuery] the EPSG codes of two input RDDs are different. Please check your RDD constructors.");
            }
        }
    }

    private static <U extends Geometry, T extends Geometry> void verifyPartitioningMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD)
            throws Exception
    {
        Objects.requireNonNull(spatialRDD.spatialPartitionedRDD, "[JoinQuery] spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        Objects.requireNonNull(queryRDD.spatialPartitionedRDD, "[JoinQuery] queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");

        final SpatialPartitioner spatialPartitioner = spatialRDD.getPartitioner();
        final SpatialPartitioner queryPartitioner = queryRDD.getPartitioner();

        if (!queryPartitioner.equals(spatialPartitioner)) {
            throw new IllegalArgumentException("[JoinQuery] queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }

        final int spatialNumPart = spatialRDD.spatialPartitionedRDD.getNumPartitions();
        final int queryNumPart = queryRDD.spatialPartitionedRDD.getNumPartitions();
        if (spatialNumPart != queryNumPart) {
            throw new IllegalArgumentException("[JoinQuery] numbers of partitions in queryRDD and spatialRDD don't match: " + queryNumPart + " vs. " + spatialNumPart + ". Please make sure they both use the same partitioning otherwise wrong results will appear.");
        }
    }

    private static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> collectGeometriesByKey(JavaPairRDD<U, T> input)
    {
        return input.groupBy(t -> GeomUtils.hashCode(t._1)).values().<U, List<T>>mapToPair(t -> {
            List<T> values = new ArrayList<T>();
            Iterator<Tuple2<U, T>> it = t.iterator();
            Tuple2<U, T> firstTpl = it.next();
            U key = firstTpl._1;
            values.add(firstTpl._2);
            while (it.hasNext()) { values.add(it.next()._2); }
            return new Tuple2<U, List<T>>(key, values);
        });
    }

    private static <U extends Geometry, T extends Geometry> JavaPairRDD<U, Long> countGeometriesByKey(JavaPairRDD<U, T> input)
    {
        return input.aggregateByKey(
                0L,
                new Function2<Long, T, Long>()
                {

                    @Override
                    public Long call(Long count, T t)
                            throws Exception
                    {
                        return count + 1;
                    }
                },
                new Function2<Long, Long, Long>()
                {

                    @Override
                    public Long call(Long count1, Long count2)
                            throws Exception
                    {
                        return count1 + count2;
                    }
                });
    }

    /**
     * Inner joins two sets of geometries on 'contains' or 'intersects' relationship.
     * <p>
     * If {@code considerBoundaryIntersection} is {@code true}, returns pairs of geometries
     * which intersect. Otherwise, returns pairs of geometries where first geometry contains second geometry.
     * <p>
     * If {@code useIndex} is false, the join uses nested loop algorithm to identify matching geometries.
     * <p>
     * If {@code useIndex} is true, the join scans query windows and uses an index of geometries
     * built prior to invoking the join to lookup matches.
     * <p>
     * Duplicate geometries present in the input queryWindowRDD, regardless of their non-spatial attributes, will not be reflected in the join results.
     * Duplicate geometries present in the input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join results.
     * @param <U> Type of the geometries in queryWindowRDD set
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries which serve as query windows
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'contains' otherwise
     * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> SpatialJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
        final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, List<T>> SpatialJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams)
            throws Exception
    {
        final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    /**
     * Inner joins two sets of geometries on 'contains' or 'intersects' relationship. Results are put in a flat pair format.
     * <p>
     * If {@code considerBoundaryIntersection} is {@code true}, returns pairs of geometries
     * which intersect. Otherwise, returns pairs of geometries where first geometry contains second geometry.
     * <p>
     * If {@code useIndex} is false, the join uses nested loop algorithm to identify matching geometries.
     * <p>
     * If {@code useIndex} is true, the join scans query windows and uses an index of geometries
     * built prior to invoking the join to lookup matches.
     * <p>
     * Duplicates present in the input RDDs will be reflected in the join results.
     *
     * @param <U> Type of the geometries in queryWindowRDD set
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries which serve as query windows
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'contains' otherwise
     * @return RDD of pairs of matching geometries
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, T> SpatialJoinQueryFlat(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams params = new JoinParams(useIndex, considerBoundaryIntersection);
        return spatialJoin(queryRDD, spatialRDD, params);
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, T> SpatialJoinQueryFlat(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams)
            throws Exception
    {
        return spatialJoin(queryRDD, spatialRDD, joinParams);
    }

    /**
     * {@link #SpatialJoinQueryFlat(SpatialRDD, SpatialRDD, boolean, boolean)} count by key.
     * <p>
     * Duplicate geometries present in the input queryWindowRDD RDD, regardless of their non-spatial attributes, will not be reflected in the join results.
     * Duplicate geometries present in the input spatialRDD RDD, regardless of their non-spatial attributes, will be reflected in the join results.
     *
     * @param <U> Type of the geometries in queryWindowRDD set
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries which serve as query windows
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'contains' otherwise
     * @return the result of {@link #SpatialJoinQueryFlat(SpatialRDD, SpatialRDD, boolean, boolean)}, but in this pair RDD, each pair contains a geometry and the count of matching geometries
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, Long> SpatialJoinQueryCountByKey(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
        final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
        return countGeometriesByKey(joinResults);
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, Long> SpatialJoinQueryCountByKey(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams)
            throws Exception
    {
        final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
        return countGeometriesByKey(joinResults);
    }

    /**
     * Inner joins two sets of geometries on 'within' relationship (aka. distance join). Results are put in a flat pair format.
     * <p>
     * If {@code considerBoundaryIntersection} is {@code true}, returns pairs of circle/geometry
     * which intersect. Otherwise, returns pairs of geometries where first circle contains second geometry.
     * <p>
     * If {@code useIndex} is false, the join uses nested loop algorithm to identify matching circle/geometry.
     * <p>
     * If {@code useIndex} is true, the join scans circles and uses an index of geometries
     * built prior to invoking the join to lookup matches.
     * <p>
     * Duplicates present in the input RDDs will be reflected in the join results.
     *
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection consider boundary intersection
     * @return RDD of pairs of matching geometries
     * @throws Exception the exception
     */
    public static <T extends Geometry> JavaPairRDD<Geometry, T> DistanceJoinQueryFlat(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
        return distanceJoin(spatialRDD, queryRDD, joinParams);
    }

    public static <T extends Geometry> JavaPairRDD<Geometry, T> DistanceJoinQueryFlat(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams)
            throws Exception
    {
        return distanceJoin(spatialRDD, queryRDD, joinParams);
    }

    /**
     * Inner joins two sets of geometries on 'within' relationship (aka. distance join).
     * The query window objects are converted to circle objects. The radius is the given distance.
     * Eventually, the original window objects are recovered and outputted.
     * <p>
     * If {@code considerBoundaryIntersection} is {@code true}, returns pairs of circle/geometry
     * which intersect. Otherwise, returns pairs of geometries where first circle contains second geometry.
     * <p>
     * If {@code useIndex} is false, the join uses nested loop algorithm to identify matching circle/geometry.
     * <p>
     * If {@code useIndex} is true, the join scans circles and uses an index of geometries
     * built prior to invoking the join to lookup matches.
     * <p>
     * Duplicate geometries present in the input CircleRDD, regardless of their non-spatial attributes, will not be reflected in the join results.
     * Duplicate geometries present in the input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join results.
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection consider boundary intersection
     * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
     * @throws Exception the exception
     */
    public static <T extends Geometry> JavaPairRDD<Geometry, List<T>> DistanceJoinQuery(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
        JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    public static <T extends Geometry> JavaPairRDD<Geometry, List<T>> DistanceJoinQuery(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams)
            throws Exception
    {
        JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    /**
     * {@link #DistanceJoinQueryFlat(SpatialRDD, CircleRDD, boolean, boolean)} count by key.
     * <p>
     * Duplicate geometries present in the input CircleRDD, regardless of their non-spatial attributes, will not be reflected in the join results.
     * Duplicate geometries present in the input spatialRDD, regardless of their non-spatial attributes, will be reflected in the join results.
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection consider boundary intersection
     * @return the result of {@link #DistanceJoinQueryFlat(SpatialRDD, CircleRDD, boolean, boolean)}, but in this pair RDD, each pair contains a geometry and the count of matching geometries
     * @throws Exception the exception
     */
    public static <T extends Geometry> JavaPairRDD<Geometry, Long> DistanceJoinQueryCountByKey(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection);
        final JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
        return countGeometriesByKey(joinResults);
    }

    public static <T extends Geometry> JavaPairRDD<Geometry, Long> DistanceJoinQueryCountByKey(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams)
            throws Exception
    {
        final JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
        return countGeometriesByKey(joinResults);
    }

    /**
     * <p>
     * Note: INTERNAL FUNCTION. API COMPATIBILITY IS NOT GUARANTEED. DO NOT USE IF YOU DON'T KNOW WHAT IT IS.
     * </p>
     */
    public static <T extends Geometry> JavaPairRDD<Geometry, T> distanceJoin(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams)
            throws Exception
    {
        JavaPairRDD<Circle, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
        return joinResults.mapToPair(new PairFunction<Tuple2<Circle, T>, Geometry, T>()
        {
            @Override
            public Tuple2<Geometry, T> call(Tuple2<Circle, T> circleTTuple2)
                    throws Exception
            {
                return new Tuple2<Geometry, T>(circleTTuple2._1().getCenterGeometry(), circleTTuple2._2());
            }
        });
    }

    /**
     * <p>
     * Note: INTERNAL FUNCTION. API COMPATIBILITY IS NOT GUARANTEED. DO NOT USE IF YOU DON'T KNOW WHAT IT IS.
     * </p>
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, T> spatialJoin(
            SpatialRDD<U> leftRDD,
            SpatialRDD<T> rightRDD,
            JoinParams joinParams)
            throws Exception
    {

        verifyCRSMatch(leftRDD, rightRDD);
        verifyPartitioningMatch(leftRDD, rightRDD);

        SparkContext sparkContext = leftRDD.spatialPartitionedRDD.context();
        Metric buildCount = Metrics.createMetric(sparkContext, "buildCount");
        Metric streamCount = Metrics.createMetric(sparkContext, "streamCount");
        Metric resultCount = Metrics.createMetric(sparkContext, "resultCount");
        Metric candidateCount = Metrics.createMetric(sparkContext, "candidateCount");

        final SpatialPartitioner partitioner =
                (SpatialPartitioner) rightRDD.spatialPartitionedRDD.partitioner().get();
        final DedupParams dedupParams = partitioner.getDedupParams();
        final SparkContext cxt = leftRDD.rawSpatialRDD.context();

        final JavaRDD<Pair<U, T>> joinResult;
        if (joinParams.useIndex) {
            if (rightRDD.indexedRDD != null) {
                final RightIndexLookupJudgement judgement =
                        new RightIndexLookupJudgement(joinParams.considerBoundaryIntersection, dedupParams);
                judgement.broadcastDedupParams(cxt);
                joinResult = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement);
            }
            else if (leftRDD.indexedRDD != null) {
                final LeftIndexLookupJudgement judgement =
                        new LeftIndexLookupJudgement(joinParams.considerBoundaryIntersection, dedupParams);
                judgement.broadcastDedupParams(cxt);
                joinResult = leftRDD.indexedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
            }
            else {
                log.warn("UseIndex is true, but no index exists. Will build index on the fly.");
                DynamicIndexLookupJudgement judgement =
                        new DynamicIndexLookupJudgement(
                                joinParams.considerBoundaryIntersection,
                                joinParams.indexType,
                                joinParams.joinBuildSide,
                                dedupParams,
                                buildCount, streamCount, resultCount, candidateCount);
                judgement.broadcastDedupParams(cxt);
                joinResult = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
            }
        }
        else {
            NestedLoopJudgement judgement = new NestedLoopJudgement(joinParams.considerBoundaryIntersection, dedupParams);
            judgement.broadcastDedupParams(cxt);
            joinResult = rightRDD.spatialPartitionedRDD.zipPartitions(leftRDD.spatialPartitionedRDD, judgement);
        }

        return joinResult.mapToPair(new PairFunction<Pair<U, T>, U, T>()
        {
            @Override
            public Tuple2<U, T> call(Pair<U, T> pair)
                    throws Exception
            {
                return new Tuple2<>(pair.getKey(), pair.getValue());
            }
        });
    }

    public static final class JoinParams
    {
        public final boolean useIndex;
        public final boolean considerBoundaryIntersection;
        public final IndexType indexType;
        public final JoinBuildSide joinBuildSide;

        public JoinParams(boolean useIndex, boolean considerBoundaryIntersection)
        {
            this(useIndex, considerBoundaryIntersection, IndexType.RTREE, JoinBuildSide.RIGHT);
        }

        public JoinParams(boolean useIndex, boolean considerBoundaryIntersection, IndexType polygonIndexType, JoinBuildSide joinBuildSide)
        {
            this.useIndex = useIndex;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.indexType = polygonIndexType;
            this.joinBuildSide = joinBuildSide;
        }
    }
}

