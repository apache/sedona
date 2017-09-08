/**
 * FILE: JoinQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.JoinQuery.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.geometryObjects.PairGeometry;
import org.datasyslab.geospark.joinJudgement.GeometryByCircleJudgement;
import org.datasyslab.geospark.joinJudgement.GeometryByCircleJudgementUsingIndex;
import org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgement;
import org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgementUsingIndex;
import org.datasyslab.geospark.spatialPartitioning.DuplicatesHandler;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;

// TODO: Auto-generated Javadoc
/**
 * The Class JoinQuery.
 */
public class JoinQuery implements Serializable{

    /**
     * Execute spatial join using index.
     *
     * @param spatialRDD                   the spatial RDD
     * @param queryRDD                     the query RDD
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    private static <T extends Geometry> JavaPairRDD<Polygon, HashSet<T>> executeSpatialJoinUsingIndex(SpatialRDD spatialRDD, SpatialRDD<Polygon> queryRDD, boolean considerBoundaryIntersection) throws Exception {
        // Check CRS information before doing calculation. The two input RDDs are supposed to have the same EPSG code if they require CRS transformation.
        if (spatialRDD.getCRStransformation() != queryRDD.getCRStransformation()) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]one input RDD doesn't perform necessary CRS transformation. Please check your RDD constructors.");
        } else if (spatialRDD.getCRStransformation() == true && queryRDD.getCRStransformation() == true) {
            if (spatialRDD.getTargetEpgsgCode().equalsIgnoreCase(queryRDD.getTargetEpgsgCode()) == false) {
                throw new Exception("[JoinQuery][SpatialJoinQuery] the EPSG codes of two input RDDs are different. Please check your RDD constructors.");
            }
        }

        //Check if rawPointRDD have index.
        if (spatialRDD.indexedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
        }
        if (spatialRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        } else if (queryRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
        } else if (queryRDD.grids!=null&&spatialRDD.grids!=null&&!queryRDD.grids.equals(spatialRDD.grids)) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }


        JavaRDD<PairGeometry<Polygon, T>> cogroupResult = spatialRDD.indexedRDD.zipPartitions(queryRDD.spatialPartitionedRDD, new GeometryByPolygonJudgementUsingIndex(considerBoundaryIntersection));

        JavaPairRDD<Polygon, HashSet<T>> joinResultWithDuplicates = cogroupResult.mapToPair(new PairFunction<PairGeometry<Polygon, T>, Polygon, HashSet<T>>() {
            @Override
            public Tuple2<Polygon, HashSet<T>> call(PairGeometry<Polygon, T> pairGeometry) throws Exception {
                return pairGeometry.makeTuple2();
            }
        });
        return joinResultWithDuplicates;
    }

    /**
     * Execute spatial join no index.
     *
     * @param spatialRDD                   the spatial RDD
     * @param queryRDD                     the query RDD
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    private static <T extends Geometry> JavaPairRDD<Polygon, HashSet<T>> executeSpatialJoinNoIndex(SpatialRDD spatialRDD, SpatialRDD<Polygon> queryRDD, boolean considerBoundaryIntersection) throws Exception {
        // Check CRS information before doing calculation. The two input RDDs are supposed to have the same EPSG code if they require CRS transformation.
        if (spatialRDD.getCRStransformation() != queryRDD.getCRStransformation()) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]one input RDD doesn't perform necessary CRS transformation. Please check your RDD constructors.");
        } else if (spatialRDD.getCRStransformation() == true && queryRDD.getCRStransformation() == true) {
            if (spatialRDD.getTargetEpgsgCode().equalsIgnoreCase(queryRDD.getTargetEpgsgCode()) == false) {
                throw new Exception("[JoinQuery][SpatialJoinQuery] the EPSG codes of two input RDDs are different. Please check your RDD constructors.");
            }
        }

        if (spatialRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        } else if (queryRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
        } else if (queryRDD.grids!=null&&spatialRDD.grids!=null&&!queryRDD.grids.equals(spatialRDD.grids)) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }

        assert spatialRDD.spatialPartitionedRDD.getNumPartitions()==queryRDD.spatialPartitionedRDD.getNumPartitions();
        JavaRDD<PairGeometry<Polygon, T>> cogroupResult = spatialRDD.spatialPartitionedRDD.zipPartitions(queryRDD.spatialPartitionedRDD, new GeometryByPolygonJudgement(considerBoundaryIntersection));

        JavaPairRDD<Polygon, HashSet<T>> joinResultWithDuplicates = cogroupResult.mapToPair(new PairFunction<PairGeometry<Polygon, T>, Polygon, HashSet<T>>() {
            @Override
            public Tuple2<Polygon, HashSet<T>> call(PairGeometry<Polygon, T> pairGeometry) throws Exception {
                return pairGeometry.makeTuple2();
            }
        });

        return joinResultWithDuplicates;
    }


    /**
     * Execute distance join using index.
     *
     * @param spatialRDD                   the spatial RDD
     * @param queryRDD                     the query RDD
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    private static <T extends Geometry> JavaPairRDD<Circle, HashSet<T>> executeDistanceJoinUsingIndex(SpatialRDD<T> spatialRDD, SpatialRDD<Circle> queryRDD, boolean considerBoundaryIntersection) throws Exception {
        // Check CRS information before doing calculation. The two input RDDs are supposed to have the same EPSG code if they require CRS transformation.
        if (spatialRDD.getCRStransformation() != queryRDD.getCRStransformation()) {
            throw new Exception("[JoinQuery][DistanceJoinQuery]one input RDD doesn't perform necessary CRS transformation. Please check your RDD constructors.");
        } else if (spatialRDD.getCRStransformation() == true && queryRDD.getCRStransformation() == true) {
            if (spatialRDD.getTargetEpgsgCode().equalsIgnoreCase(queryRDD.getTargetEpgsgCode()) == false) {
                throw new Exception("[JoinQuery][DistanceJoinQuery] the EPSG codes of two input RDDs are different. Please check your RDD constructors.");
            }
        }

        //Check if rawPointRDD have index.
        if (spatialRDD.indexedRDD == null) {
            throw new Exception("[JoinQuery][DistanceJoinQuery] Index doesn't exist. Please build index.");
        }
        if (spatialRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][DistanceJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        } else if (queryRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][DistanceJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
        } else if (queryRDD.grids!=null&&spatialRDD.grids!=null&&!queryRDD.grids.equals(spatialRDD.grids)) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }

        JavaRDD<PairGeometry<Circle, T>> cogroupResult = spatialRDD.indexedRDD.zipPartitions(queryRDD.spatialPartitionedRDD, new GeometryByCircleJudgementUsingIndex(considerBoundaryIntersection));

        JavaPairRDD<Circle, HashSet<T>> joinResultWithDuplicates = cogroupResult.mapToPair(new PairFunction<PairGeometry<Circle, T>, Circle, HashSet<T>>() {
            @Override
            public Tuple2<Circle, HashSet<T>> call(PairGeometry<Circle, T> pairGeometry) throws Exception {
                return pairGeometry.makeTuple2();
            }
        });

        return joinResultWithDuplicates;
    }

    /**
     * Execute distance join no index.
     *
     * @param spatialRDD                   the spatial RDD
     * @param queryRDD                     the query RDD
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    private static <T extends Geometry> JavaPairRDD<Circle, HashSet<T>> executeDistanceJoinNoIndex(SpatialRDD<T> spatialRDD, SpatialRDD<Circle> queryRDD, boolean considerBoundaryIntersection) throws Exception {
        // Check CRS information before doing calculation. The two input RDDs are supposed to have the same EPSG code if they require CRS transformation.
        if (spatialRDD.getCRStransformation() != queryRDD.getCRStransformation()) {
            throw new Exception("[JoinQuery][DistanceJoinQuery]one input RDD doesn't perform necessary CRS transformation. Please check your RDD constructors.");
        } else if (spatialRDD.getCRStransformation() == true && queryRDD.getCRStransformation() == true) {
            if (spatialRDD.getTargetEpgsgCode().equalsIgnoreCase(queryRDD.getTargetEpgsgCode()) == false) {
                throw new Exception("[JoinQuery][DistanceJoinQuery] the EPSG codes of two input RDDs are different. Please check your RDD constructors.");
            }
        }

        if (spatialRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][DistanceJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        } else if (queryRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][DistanceJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
        } else if (queryRDD.grids!=null&&spatialRDD.grids!=null&&!queryRDD.grids.equals(spatialRDD.grids)) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }

        JavaRDD<PairGeometry<Circle, T>> cogroupResult = spatialRDD.spatialPartitionedRDD.zipPartitions(queryRDD.spatialPartitionedRDD, new GeometryByCircleJudgement(considerBoundaryIntersection));

        JavaPairRDD<Circle, HashSet<T>> joinResultWithDuplicates = cogroupResult.mapToPair(new PairFunction<PairGeometry<Circle, T>, Circle, HashSet<T>>() {
            @Override
            public Tuple2<Circle, HashSet<T>> call(PairGeometry<Circle, T> pairGeometry) throws Exception {
                return pairGeometry.makeTuple2();
            }
        });

        return joinResultWithDuplicates;
    }


    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static <T extends Geometry> JavaPairRDD<Polygon, HashSet<T>> SpatialJoinQuery(SpatialRDD<T> spatialRDD, SpatialRDD<Polygon> queryRDD, boolean useIndex, boolean considerBoundaryIntersection) throws Exception {
        JavaPairRDD<Polygon, HashSet<T>> resultWithDuplicates = SpatialJoinQueryWithDuplicates(spatialRDD, queryRDD, useIndex, considerBoundaryIntersection);
        return DuplicatesHandler.removeDuplicates(resultWithDuplicates);
    }

    public static <T extends Geometry> JavaPairRDD<Polygon, HashSet<T>> SpatialJoinQueryWithDuplicates(SpatialRDD<T> spatialRDD, SpatialRDD<Polygon> queryRDD, boolean useIndex, boolean considerBoundaryIntersection) throws Exception {
        JavaPairRDD<Polygon, HashSet<T>> resultWithDuplicates;
        if(useIndex)
        {
            resultWithDuplicates = executeSpatialJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection);
        }
        else
        {
            resultWithDuplicates = executeSpatialJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection);
        }
        return resultWithDuplicates;
    }

    /**
     * Spatial join query count by key.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static <T extends Geometry> JavaPairRDD<Polygon, Long> SpatialJoinQueryCountByKey(SpatialRDD<T> spatialRDD, SpatialRDD<Polygon> queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        JavaPairRDD<Polygon, HashSet<T>> joinResults = SpatialJoinQuery(spatialRDD, queryRDD, useIndex, considerBoundaryIntersection);
        JavaPairRDD<Polygon, Long> resultCountByKey = joinResults.mapValues(new Function<HashSet<T>,Long>()
        {
            @Override
            public Long call(HashSet<T> spatialObjects) throws Exception {

                return (long) spatialObjects.size();
            }
        });
        return resultCountByKey;
    }

    private static <T extends Geometry> JavaPairRDD<T, HashSet<T>> DistanceJoinQueryImpl(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection, boolean allowDuplicates) throws Exception {
        JavaPairRDD<Circle, HashSet<T>> joinResult = DistanceJoinQueryRawImpl(spatialRDD, queryRDD, useIndex, considerBoundaryIntersection, allowDuplicates);

        return joinResult.mapToPair(new PairFunction<Tuple2<Circle,HashSet<T>>,T,HashSet<T>>()
        {
            @Override
            public Tuple2<T, HashSet<T>> call(Tuple2<Circle, HashSet<T>> pairObjects) throws Exception {
                return new Tuple2<>((T)pairObjects._1.getCenterGeometry(), pairObjects._2());
            }
        });
    }

    private static <T extends Geometry> JavaPairRDD<Circle, HashSet<T>> DistanceJoinQueryRawImpl(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex, boolean considerBoundaryIntersection, boolean allowDuplicates) throws Exception {
        JavaPairRDD<Circle, HashSet<T>> result;
        if(useIndex)
        {
            result = executeDistanceJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection);
        }
        else
        {
            result = executeDistanceJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection);
        }

        if (!allowDuplicates) {
            result = DuplicatesHandler.removeDuplicates(result);
        }
        return result;
    }

    public static <T extends Geometry> JavaPairRDD<T, HashSet<T>> DistanceJoinQuery(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        return DistanceJoinQueryImpl(spatialRDD, queryRDD, useIndex, considerBoundaryIntersection, false);
    }

    public static <T extends Geometry> JavaPairRDD<T, HashSet<T>> DistanceJoinQueryWithDuplicates(SpatialRDD<T> spatialRDD, CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        return DistanceJoinQueryImpl(spatialRDD, queryRDD, useIndex, considerBoundaryIntersection, true);
    }

    public static <T extends Geometry> JavaPairRDD<T, Long> DistanceJoinQueryCountByKey(SpatialRDD<T> spatialRDD,CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        final JavaPairRDD<Circle, HashSet<T>> joinResult = DistanceJoinQueryRawImpl(spatialRDD, queryRDD, useIndex, considerBoundaryIntersection, false);
        return joinResult.mapToPair(new PairFunction<Tuple2<Circle,HashSet<T>>,T,Long>()
        {
            @Override
            public Tuple2<T, Long> call(Tuple2<Circle, HashSet<T>> pairObjects) throws Exception {
                return new Tuple2<>((T)pairObjects._1.getCenterGeometry(),(long) pairObjects._2.size());
            }
        });
    }
}

