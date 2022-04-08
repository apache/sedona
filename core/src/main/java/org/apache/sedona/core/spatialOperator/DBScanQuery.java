package org.apache.sedona.core.spatialOperator;

import org.apache.sedona.core.dbscanJudgement.DBScanJudgement;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;

public class DBScanQuery
        implements Serializable
{
    public static <T extends Geometry> List<Integer> SpatialDBScanQuery(SpatialRDD<T> spatialRDD, double eps, int minPoints, boolean useIndex)
    {
        if (useIndex) {
            if (spatialRDD.indexedRawRDD == null) {
                throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
            }
            // TODO: Add implementation with index
            return null;
        }
        else {
            JavaRDD<Integer> result = spatialRDD.getRawSpatialRDD().repartition(1).mapPartitions(new DBScanJudgement(eps, minPoints), true);
            return result.collect();
        }
    }
}