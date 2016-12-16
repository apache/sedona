/**
 * FILE: KNNQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.KNNQuery.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.knnJudgement.PointDistanceComparator;
import org.datasyslab.geospark.knnJudgement.PointKnnJudgement;
import org.datasyslab.geospark.knnJudgement.PointKnnJudgementUsingIndex;
import org.datasyslab.geospark.knnJudgement.PolygonDistanceComparator;
import org.datasyslab.geospark.knnJudgement.PolygonKnnJudgement;
import org.datasyslab.geospark.knnJudgement.PolygonKnnJudgementUsingIndex;
import org.datasyslab.geospark.knnJudgement.RectangleDistanceComparator;
import org.datasyslab.geospark.knnJudgement.RectangleKnnJudgement;
import org.datasyslab.geospark.knnJudgement.RectangleKnnJudgementUsingIndex;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;


// TODO: Auto-generated Javadoc
/**
 * The Class KNNQuery.
 */
public class KNNQuery implements Serializable{
	
	/**
	 * Spatial knn query.
	 *
	 * @param pointRDD the point RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @return the list
	 */
	public static List<Point> SpatialKnnQuery(PointRDD pointRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		@SuppressWarnings("serial")

		JavaRDD<Point> tmp = pointRDD.getRawPointRDD().mapPartitions(new PointKnnJudgement(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new PointDistanceComparator(queryCenter));

	}
	
	/**
	 * Spatial knn query using index.
	 *
	 * @param pointRDD the point RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @return the list
	 */
	public static List<Point> SpatialKnnQueryUsingIndex(PointRDD pointRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")

        if(pointRDD.indexedRDDNoId == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
        }
		JavaRDD<Point> tmp = pointRDD.indexedRDDNoId.mapPartitions(new PointKnnJudgementUsingIndex(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new PointDistanceComparator(queryCenter));

	}
	
	/**
	 * Spatial knn query.
	 *
	 * @param objectRDD the object RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @return the list
	 */
	public static List<Envelope> SpatialKnnQuery(RectangleRDD objectRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		@SuppressWarnings("serial")

		JavaRDD<Envelope> tmp = objectRDD.getRawRectangleRDD().mapPartitions(new RectangleKnnJudgement(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new RectangleDistanceComparator(queryCenter));

	}
	
	/**
	 * Spatial knn query using index.
	 *
	 * @param objectRDD the object RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @return the list
	 */
	public static List<Envelope> SpatialKnnQueryUsingIndex(RectangleRDD objectRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")

        if(objectRDD.indexedRDDNoId == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
        }
		JavaRDD<Envelope> tmp = objectRDD.indexedRDDNoId.mapPartitions(new RectangleKnnJudgementUsingIndex(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new RectangleDistanceComparator(queryCenter));

	}
	
	/**
	 * Spatial knn query.
	 *
	 * @param objectRDD the object RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @return the list
	 */
	public static List<Polygon> SpatialKnnQuery(PolygonRDD objectRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		@SuppressWarnings("serial")

		JavaRDD<Polygon> tmp = objectRDD.getRawPolygonRDD().mapPartitions(new PolygonKnnJudgement(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new PolygonDistanceComparator(queryCenter));

	}
	
	/**
	 * Spatial knn query using index.
	 *
	 * @param objectRDD the object RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @return the list
	 */
	public static List<Polygon> SpatialKnnQueryUsingIndex(PolygonRDD objectRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")

        if(objectRDD.indexedRDDNoId == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
        }
		JavaRDD<Polygon> tmp = objectRDD.indexedRDDNoId.mapPartitions(new PolygonKnnJudgementUsingIndex(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new PolygonDistanceComparator(queryCenter));

	}
}
