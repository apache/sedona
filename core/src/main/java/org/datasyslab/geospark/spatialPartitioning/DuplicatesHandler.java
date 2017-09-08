/**
 * FILE: DuplicatesHandler.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.DuplicatesHandler.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.HashSet;

// TODO: Auto-generated Javadoc
/**
 * The Class DuplicatesHandler.
 */
public class DuplicatesHandler {
    
    /**
     * Removes the duplicates geometry by polygon.
     *
     * @param joinResultBeforeAggregation the join result before aggregation
     * @return the java pair RDD
     */
    public static <K extends Geometry, T extends Geometry> JavaPairRDD<K, HashSet<T>> removeDuplicates(JavaPairRDD<K, HashSet<T>> joinResultBeforeAggregation) {
            //AggregateByKey?
            JavaPairRDD<K, HashSet<T>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<T>, HashSet<T>, HashSet<T>>() {
                @Override
                public HashSet<T> call(HashSet<T> geometries, HashSet<T> otherGeometries) throws Exception {
                	geometries.addAll(otherGeometries);
                    return geometries;
                }
            });
        return joinResultAfterAggregation;
    }
}
