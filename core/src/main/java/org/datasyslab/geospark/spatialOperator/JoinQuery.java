/**
 * FILE: JoinQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.JoinQuery.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.joinJudgement.DynamicIndexLookupJudgement;
import org.datasyslab.geospark.joinJudgement.IndexLookupJudgement;
import org.datasyslab.geospark.joinJudgement.NestedLoopJudgement;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;

// TODO: Auto-generated Javadoc
/**
 * The Class JoinQuery.
 */
public class JoinQuery implements Serializable{

    private static <T extends Geometry, U extends Geometry> void verifyCRSMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD) throws Exception {
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

    private static <T extends Geometry, U extends Geometry> void verifyPartitioningMatch(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD) throws Exception {
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

    private static <T extends Geometry, U extends Geometry> JavaPairRDD<U, HashSet<T>> collectGeometriesByKey(JavaPairRDD<U, T> input) {
        return input.aggregateByKey(
            new HashSet<T>(),
            new Function2<HashSet<T>, T, HashSet<T>>() {
                @Override
                public HashSet<T> call(HashSet<T> ts, T t) throws Exception {
                    ts.add(t);
                    return ts;
                }
            },
            new Function2<HashSet<T>, HashSet<T>, HashSet<T>>() {
                @Override
                public HashSet<T> call(HashSet<T> ts, HashSet<T> ts2) throws Exception {
                    ts.addAll(ts2);
                    return ts;
                }
            });
    }

    private static <T extends Geometry, U extends Geometry> JavaPairRDD<U, Long> countGeometriesByKey(JavaPairRDD<U, T> input) {
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

    private static final class JoinParamsInternal {
        public final boolean useIndex;
        public final boolean considerBoundaryIntersection;
        public final boolean allowDuplicates;
        public final IndexType polygonIndexType;

        private JoinParamsInternal(boolean useIndex, boolean considerBoundaryIntersection, boolean allowDuplicates) {
            this.useIndex = useIndex;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = allowDuplicates;
            this.polygonIndexType = null;
        }

        private JoinParamsInternal(boolean considerBoundaryIntersection, IndexType polygonIndexType) {
            this.useIndex = false;
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.allowDuplicates = false;
            this.polygonIndexType = polygonIndexType;
        }
    }

    private static <T extends Geometry, U extends Geometry> JavaPairRDD<U, T> spatialJoinInt(
        SpatialRDD<T> spatialRDD,
        SpatialRDD<U> queryRDD,
        JoinParamsInternal joinParams) throws Exception {

        verifyCRSMatch(spatialRDD, queryRDD);
        verifyPartitioningMatch(spatialRDD, queryRDD);

        final SpatialPartitioner partitioner =
            (SpatialPartitioner) spatialRDD.spatialPartitionedRDD.partitioner().get();
        final DedupParams dedupParams = partitioner.getDedupParams();

        final JavaRDD<Pair<U, T>> resultWithDuplicates;
        if(joinParams.useIndex) {
            Objects.requireNonNull(spatialRDD.indexedRDD, "[JoinQuery] Index doesn't exist. Please build index.");

            final IndexLookupJudgement judgement =
                new IndexLookupJudgement(joinParams.considerBoundaryIntersection, dedupParams);
            resultWithDuplicates = spatialRDD.indexedRDD.zipPartitions(queryRDD.spatialPartitionedRDD, judgement);

        } else {
            final FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<U, T>> judgement;
            if (joinParams.polygonIndexType != null) {
                judgement = new DynamicIndexLookupJudgement(
                    joinParams.considerBoundaryIntersection, joinParams.polygonIndexType, dedupParams);
            } else {
                judgement = new NestedLoopJudgement(joinParams.considerBoundaryIntersection, dedupParams);
            }
            resultWithDuplicates = spatialRDD.spatialPartitionedRDD.zipPartitions(queryRDD.spatialPartitionedRDD, judgement);
        }

        final boolean uniqueResults = dedupParams != null;

        final JavaRDD<Pair<U, T>> result =
            (joinParams.allowDuplicates || uniqueResults) ? resultWithDuplicates
                : resultWithDuplicates.distinct();

        return result.mapToPair(new PairFunction<Pair<U, T>, U, T>() {
            @Override
            public Tuple2<U, T> call(Pair<U, T> pair) throws Exception {
                return new Tuple2<>(pair.getKey(), pair.getValue());
            }
        });
    }

    /**
     * Joins two sets of geometries on 'contains' or 'intersects' relationship.
     *
     * One set of geometries must be a set of polygons.
     *
     * If {@code considerBoundaryIntersection} is {@code true}, returns pairs of geometries
     * which intersect. Otherwise, returns pairs of geometries where first geometry contains second geometry.
     *
     * If {@code useIndex} is false, the join uses nested loop algorithm to identify matching geometries.
     *
     * If {@code useIndex} is true, the join scans polygons and uses an index of geometries
     * built prior to invoking the join to lookup matches.
     *
     * Because the results are reported as a HashSet, any duplicates in the original spatialRDD will
     * be eliminated.
     *
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of polygons
     * @param useIndex Boolean indicating whether the join should use the index from {@code spatialRDD.indexedRDD}
     * @param considerBoundaryIntersection Join relationship type: 'intersects' if true, 'contains' otherwise
     * @param <T> Type of the geometries in spatialRDD set
     * @return RDD of pairs where each pair contains a polygon and a set of matching geometries
     */
    public static <T extends Geometry> JavaPairRDD<Polygon, HashSet<T>> SpatialJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<Polygon> queryRDD, boolean useIndex, boolean considerBoundaryIntersection) throws Exception {
        final JoinParamsInternal joinParams = new JoinParamsInternal(useIndex, considerBoundaryIntersection, false);
        final JavaPairRDD<Polygon, T> joinResults = spatialJoinInt(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    /**
     * A faster version of SpatialJoinQuery which may produce duplicate results.
     */
    public static <T extends Geometry> JavaPairRDD<Polygon, HashSet<T>> SpatialJoinQueryWithDuplicates(SpatialRDD<T> spatialRDD, SpatialRDD<Polygon> queryRDD, boolean useIndex, boolean considerBoundaryIntersection) throws Exception {
        final JoinParamsInternal joinParams = new JoinParamsInternal(useIndex, considerBoundaryIntersection, true);
        final JavaPairRDD<Polygon, T> joinResults = spatialJoinInt(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(joinResults);
    }

    public static final class JoinParams {
        public final boolean considerBoundaryIntersection;
        public final IndexType polygonIndexType;

        public JoinParams(boolean considerBoundaryIntersection) {
            this(considerBoundaryIntersection, null);
        }

        public JoinParams(boolean considerBoundaryIntersection, IndexType polygonIndexType) {
            this.considerBoundaryIntersection = considerBoundaryIntersection;
            this.polygonIndexType = polygonIndexType;
        }
    }

    /**
     * Joins two sets of geometries on 'contains' or 'intersects' relationship.
     *
     * If {@link JoinParams#considerBoundaryIntersection} is {@code true}, returns pairs of geometries
     * which intersect. Otherwise, returns pairs of geometries where first geometry contains second geometry.
     *
     * By default, the join uses nested loop algorithm to find pairs of matching geometries.
     *
     * If {@link JoinParams#polygonIndexType} is specified, the join builds an index of the specified type
     * from polygons partition, then scans the other partition and looks up matching polygons in the index.
     *
     * Duplicates present in the input RDDs will be reflected in the join results.
     *
     * @param spatialRDD Set of geometries
     * @param queryRDD Set of polygons
     * @param joinParams Join parameters including relationship type ('contains' vs. 'intersects')
     * @param <T> Type of the geometries in spatialRDD set
     * @return RDD of pairs of matching geometries
     */
    public static <T extends Geometry, U extends Geometry> JavaPairRDD<U, T> spatialJoin(SpatialRDD<T> spatialRDD, SpatialRDD<U> queryRDD, JoinParams joinParams) throws Exception {
        final JoinParamsInternal params = new JoinParamsInternal(joinParams.considerBoundaryIntersection, joinParams.polygonIndexType);
        return spatialJoinInt(spatialRDD, queryRDD, params);
    }

    /**
     * Spatial join query count by key.
     *
     * Duplicates present in the input RDDs will be reflected in the join results.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static <T extends Geometry> JavaPairRDD<Polygon, Long> SpatialJoinQueryCountByKey(SpatialRDD<T> spatialRDD, SpatialRDD<Polygon> queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        final JoinParamsInternal joinParams = new JoinParamsInternal(useIndex, considerBoundaryIntersection, false);
        final JavaPairRDD<Polygon, T> joinResults = spatialJoinInt(spatialRDD, queryRDD, joinParams);
        return countGeometriesByKey(joinResults);
    }

    public static <T extends Geometry> JavaPairRDD<T, HashSet<T>> DistanceJoinQuery(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        final JoinParamsInternal joinParams = new JoinParamsInternal(useIndex, considerBoundaryIntersection, false);
        return distanceJoinInt(spatialRDD, queryRDD, joinParams);
    }

    public static <T extends Geometry> JavaPairRDD<T, HashSet<T>> DistanceJoinQueryWithDuplicates(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        final JoinParamsInternal joinParams = new JoinParamsInternal(useIndex, considerBoundaryIntersection, true);
        return distanceJoinInt(spatialRDD, queryRDD, joinParams);
    }

    public static <T extends Geometry> JavaPairRDD<T, Long> DistanceJoinQueryCountByKey(SpatialRDD<T> spatialRDD,CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        final JoinParamsInternal joinParams = new JoinParamsInternal(useIndex, considerBoundaryIntersection, false);
        final JavaPairRDD<Circle, T> joinResults = spatialJoinInt(spatialRDD, queryRDD, joinParams);
        return countGeometriesByKey(
            joinResults.mapToPair(new PairFunction<Tuple2<Circle, T>, T, T>() {
                @Override
                public Tuple2<T, T> call(Tuple2<Circle, T> tuple) throws Exception {
                    return new Tuple2<>((T) tuple._1().getCenterGeometry(), tuple._2);
                }
            }));
    }

    private static <T extends Geometry> JavaPairRDD<T, HashSet<T>> distanceJoinInt(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, JoinQuery.JoinParamsInternal joinParams) throws Exception {
        final JavaPairRDD<Circle, T> joinResults = spatialJoinInt(spatialRDD, queryRDD, joinParams);
        return collectGeometriesByKey(
            joinResults.mapToPair(new PairFunction<Tuple2<Circle, T>, T, T>() {
                @Override
                public Tuple2<T, T> call(Tuple2<Circle, T> tuple) throws Exception {
                    return new Tuple2<>((T) tuple._1().getCenterGeometry(), tuple._2);
                }
            }));
    }
}

