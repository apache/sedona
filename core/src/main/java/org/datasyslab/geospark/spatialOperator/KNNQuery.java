/**
 * FILE: KNNQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.KNNQuery.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.knnJudgement.GeometryDistanceComparator;
import org.datasyslab.geospark.knnJudgement.KnnJudgement;
import org.datasyslab.geospark.knnJudgement.KnnJudgementUsingIndex;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geospark.utils.CRSTransformation;

import java.io.Serializable;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class KNNQuery.
 */
public class KNNQuery
        implements Serializable
{

    /**
     * Spatial knn query.
     *
     * @param spatialRDD the spatial RDD
     * @param originalQueryPoint the original query window
     * @param k the k
     * @param useIndex the use index
     * @return the list
     */
    public static <U extends Geometry, T extends Geometry> List<T> SpatialKnnQuery(SpatialRDD<T> spatialRDD, U originalQueryPoint, Integer k, boolean useIndex)
    {
        U queryCenter = originalQueryPoint;
        if (spatialRDD.getCRStransformation()) {
            queryCenter = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(), spatialRDD.getTargetEpgsgCode(), originalQueryPoint);
        }

        if (useIndex) {
            if (spatialRDD.indexedRawRDD == null) {
                throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
            }
            JavaRDD<T> tmp = spatialRDD.indexedRawRDD.mapPartitions(new KnnJudgementUsingIndex(queryCenter, k));
            List<T> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter, true));
            // Take the top k
            return result;
        }
        else {
            JavaRDD<T> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new KnnJudgement(queryCenter, k));
            List<T> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter, true));
            // Take the top k
            return result;
        }
    }
}
