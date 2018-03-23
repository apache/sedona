/*
 * FILE: JoinQuery
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.JoinBuildSide;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.joinJudgement.DynamicIndexLookupJudgement;
import org.datasyslab.geospark.joinJudgement.LeftIndexLookupJudgement;
import org.datasyslab.geospark.joinJudgement.NestedLoopJudgement;
import org.datasyslab.geospark.joinJudgement.RightIndexLookupJudgement;
import org.datasyslab.geospark.monitoring.GeoSparkMetric;
import org.datasyslab.geospark.monitoring.GeoSparkMetrics;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import java.util.HashSet;
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

    private static <U extends Geometry, T extends Geometry> JavaPairRDD<U, HashSet<T>> collectGeometriesByKey(JavaPairRDD<U, T> input)
    {
        return input.aggregateByKey(
                new HashSet<T>(),
                new Function2<HashSet<T>, T, HashSet<T>>()
                {
                    @Override
                    public HashSet<T> call(HashSet<T> ts, T t)
                            throws Exception
                    {
                        ts.add(t);
                        return ts;
                    }
                },
                new Function2<HashSet<T>, HashSet<T>, HashSet<T>>()
                {
                    @Override
                    public HashSet<T> call(HashSet<T> ts, HashSet<T> ts2)
                            throws Exception
                    {
                        ts.addAll(ts2);
                        return ts;
                    }
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

    public static final class JoinParams
    {
        public final boolean useIndex;
        public final boolean considerBoundaryIntersection;
        public final boolean allowDuplicates;
        public final IndexType indexType;
        public final JoinBuildSide joinBuildSide;

        public JoinParams(boolean useIndex, boolean considerBoundaryIntersection, boolean allowDuplicates)
        {
            this.useIndex = useIndex;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = allowDuplicates;
            this.indexType = IndexType.RTREE;
            this.joinBuildSide = JoinBuildSide.RIGHT;
        }

        public JoinParams(boolean considerBoundaryIntersection, IndexType polygonIndexType, JoinBuildSide joinBuildSide)
        {
            this.useIndex = false;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = false;
            this.indexType = polygonIndexType;
            this.joinBuildSide = joinBuildSide;
        }
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
     * Because the results are reported as a HashSet, any duplicates in the original spatialRDD will
     * be eliminated.
     *
     * @param <U> Type of the geometries in queryWindowRDD set
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries which serve as query windows
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'contains' otherwise
     * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, HashSet<T>> SpatialJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, false);
        final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, HashSet<T>> SpatialJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams)
            throws Exception
    {
        final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    /**
     * A faster version of {@link #SpatialJoinQuery(SpatialRDD, SpatialRDD, boolean, boolean)} which may produce duplicate results.
     *
     * @param <U> Type of the geometries in queryWindowRDD set
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries which serve as query windows
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'contains' otherwise
     * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, HashSet<T>> SpatialJoinQueryWithDuplicates(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, true);
        final JavaPairRDD<U, T> joinResults = spatialJoin(queryRDD, spatialRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<U, HashSet<T>> SpatialJoinQueryWithDuplicates(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams)
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
        final JoinParams params = new JoinParams(useIndex, considerBoundaryIntersection, false);
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
     * Duplicates present in the input RDDs will be reflected in the join results.
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
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, false);
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
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, false);
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
     * Because the results are reported as a HashSet, any duplicates in the original spatialRDD will
     * be eliminated.
     *
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection consider boundary intersection
     * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
     * @throws Exception the exception
     */
    public static <T extends Geometry> JavaPairRDD<Geometry, HashSet<T>> DistanceJoinQuery(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, false);
        JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    public static <T extends Geometry> JavaPairRDD<Geometry, HashSet<T>> DistanceJoinQuery(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams)
            throws Exception
    {
        JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    /**
     * A faster version of {@link #DistanceJoinQuery(SpatialRDD, CircleRDD, boolean, boolean)} which may produce duplicate results.
     *
     * @param <T> Type of the geometries in spatialRDD set
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of geometries
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection consider boundary intersection
     * @return RDD of pairs where each pair contains a geometry and a set of matching geometries
     * @throws Exception the exception
     */
    public static <T extends Geometry> JavaPairRDD<Geometry, HashSet<T>> DistanceJoinQueryWithDuplicates(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex, boolean considerBoundaryIntersection)
            throws Exception
    {
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, true);
        JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    public static <T extends Geometry> JavaPairRDD<Geometry, HashSet<T>> DistanceJoinQueryWithDuplicates(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinParams joinParams)
            throws Exception
    {
        JavaPairRDD<Geometry, T> joinResults = distanceJoin(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    /**
     * {@link #DistanceJoinQueryFlat(SpatialRDD, CircleRDD, boolean, boolean)} count by key.
     * <p>
     * Duplicates present in the input RDDs will be reflected in the join results.
     *
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
        final JoinParams joinParams = new JoinParams(useIndex, considerBoundaryIntersection, false);
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
                return new Tuple2<Geometry, T>((Geometry) circleTTuple2._1().getCenterGeometry(), circleTTuple2._2());
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
        GeoSparkMetric buildCount = GeoSparkMetrics.createMetric(sparkContext, "buildCount");
        GeoSparkMetric streamCount = GeoSparkMetrics.createMetric(sparkContext, "streamCount");
        GeoSparkMetric resultCount = GeoSparkMetrics.createMetric(sparkContext, "resultCount");
        GeoSparkMetric candidateCount = GeoSparkMetrics.createMetric(sparkContext, "candidateCount");

        final SpatialPartitioner partitioner =
                (SpatialPartitioner) rightRDD.spatialPartitionedRDD.partitioner().get();
        final DedupParams dedupParams = partitioner.getDedupParams();

        final JavaRDD<Pair<U, T>> resultWithDuplicates;
        if (joinParams.useIndex) {
            if (rightRDD.indexedRDD != null) {
                final RightIndexLookupJudgement judgement =
                        new RightIndexLookupJudgement(joinParams.considerBoundaryIntersection, dedupParams);
                resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.indexedRDD, judgement);
            }
            else if (leftRDD.indexedRDD != null) {
                final LeftIndexLookupJudgement judgement =
                        new LeftIndexLookupJudgement(joinParams.considerBoundaryIntersection, dedupParams);
                resultWithDuplicates = leftRDD.indexedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
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
                resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
            }
        }/*
        else if (joinParams.indexType != null) {
            DynamicIndexLookupJudgement judgement =
                new DynamicIndexLookupJudgement(
                    joinParams.considerBoundaryIntersection,
                    joinParams.indexType,
                    joinParams.joinBuildSide,
                    dedupParams,
                    buildCount, streamCount, resultCount, candidateCount);
            resultWithDuplicates = leftRDD.spatialPartitionedRDD.zipPartitions(rightRDD.spatialPartitionedRDD, judgement);
        }*/
        else {
            NestedLoopJudgement judgement = new NestedLoopJudgement(joinParams.considerBoundaryIntersection, dedupParams);
            resultWithDuplicates = rightRDD.spatialPartitionedRDD.zipPartitions(leftRDD.spatialPartitionedRDD, judgement);
        }

        final boolean uniqueResults = dedupParams != null;

        final JavaRDD<Pair<U, T>> result =
                (joinParams.allowDuplicates || uniqueResults) ? resultWithDuplicates
                        : resultWithDuplicates.distinct();

        return result.mapToPair(new PairFunction<Pair<U, T>, U, T>()
        {
            @Override
            public Tuple2<U, T> call(Pair<U, T> pair)
                    throws Exception
            {
                return new Tuple2<>(pair.getKey(), pair.getValue());
            }
        });
    }
}

