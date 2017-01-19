/**
 * FILE: DuplicatesHandler.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.DuplicatesHandler.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.util.HashSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class DuplicatesHandler.
 */
public class DuplicatesHandler {
	
    /**
     * Removes the duplicates geometry by rectangle.
     *
     * @param joinResultBeforeAggregation the join result before aggregation
     * @return the java pair RDD
     */
    public static JavaPairRDD<Envelope, HashSet<Geometry>> removeDuplicatesGeometryByRectangle(JavaPairRDD<Envelope, HashSet<Geometry>> joinResultBeforeAggregation) {
        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Geometry>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Geometry>, HashSet<Geometry>, HashSet<Geometry>>() {
            @Override
            public HashSet<Geometry> call(HashSet<Geometry> geometries, HashSet<Geometry> otherGeometries) throws Exception {
            	geometries.addAll(otherGeometries);
                return geometries;
            }
        });
        return joinResultAfterAggregation;
    }
    
    /**
     * Removes the duplicates rectangle by rectangle.
     *
     * @param joinResultBeforeAggregation the join result before aggregation
     * @return the java pair RDD
     */
    public static JavaPairRDD<Envelope, HashSet<Envelope>> removeDuplicatesRectangleByRectangle(JavaPairRDD<Envelope, HashSet<Envelope>> joinResultBeforeAggregation) {
        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Envelope>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Envelope>, HashSet<Envelope>, HashSet<Envelope>>() {
            @Override
            public HashSet<Envelope> call(HashSet<Envelope> objects, HashSet<Envelope> objects2) throws Exception {
            	objects.addAll(objects2);
                return objects;
            }
        });
     
        return joinResultAfterAggregation;
    }
    
    /**
     * Removes the duplicates geometry by polygon.
     *
     * @param joinResultBeforeAggregation the join result before aggregation
     * @return the java pair RDD
     */
    public static JavaPairRDD<Polygon, HashSet<Geometry>> removeDuplicatesGeometryByPolygon(JavaPairRDD<Polygon, HashSet<Geometry>> joinResultBeforeAggregation) {
            //AggregateByKey?
            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Geometry>, HashSet<Geometry>, HashSet<Geometry>>() {
                @Override
                public HashSet<Geometry> call(HashSet<Geometry> geometries, HashSet<Geometry> otherGeometries) throws Exception {
                	geometries.addAll(otherGeometries);
                    return geometries;
                }
            });
        return joinResultAfterAggregation;
    }
}
