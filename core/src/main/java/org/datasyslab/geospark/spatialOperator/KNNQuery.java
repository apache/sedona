/*
 * FILE: KNNQuery
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
