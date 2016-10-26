package org.datasyslab.geospark.spatialOperator;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
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
import com.vividsolutions.jts.index.strtree.STRtree;


public class KNNQuery implements Serializable{
	/**
	 * Spatial K Nearest Neighbors query
	 * @param pointRDD specify the input pointRDD
	 * @param queryCenter specify the query center 
	 * @param k specify the K
	 * @return A list which contains K nearest points
	 */
	public static List<Point> SpatialKnnQuery(PointRDD pointRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		@SuppressWarnings("serial")

		JavaRDD<Point> tmp = pointRDD.getRawPointRDD().mapPartitions(new PointKnnJudgement(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new PointDistanceComparator(queryCenter));

	}
	/**
	 * Spatial K Nearest Neighbors query using index
	 * @param pointRDD specify the input pointRDD
	 * @param queryCenter specify the query center 
	 * @param k specify the K
	 * @return A list which contains K nearest points
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
	 * Spatial K Nearest Neighbors query
	 * @param objectRDD specify the input rectangelRDD
	 * @param queryCenter specify the query center 
	 * @param k specify the K
	 * @return A list which contains K nearest points
	 */
	public static List<Envelope> SpatialKnnQuery(RectangleRDD objectRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		@SuppressWarnings("serial")

		JavaRDD<Envelope> tmp = objectRDD.getRawRectangleRDD().mapPartitions(new RectangleKnnJudgement(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new RectangleDistanceComparator(queryCenter));

	}
	/**
	 * Spatial K Nearest Neighbors query using index
	 * @param objectRDD specify the input rectangelRDD
	 * @param queryCenter specify the query center 
	 * @param k specify the K
	 * @return A list which contains K nearest points
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
	 * Spatial K Nearest Neighbors query
	 * @param objectRDD specify the input polygonRDD
	 * @param queryCenter specify the query center 
	 * @param k specify the K
	 * @return A list which contains K nearest points
	 */
	public static List<Polygon> SpatialKnnQuery(PolygonRDD objectRDD, Point queryCenter, Integer k) {
		// For each partation, build a priority queue that holds the topk
		@SuppressWarnings("serial")

		JavaRDD<Polygon> tmp = objectRDD.getRawPolygonRDD().mapPartitions(new PolygonKnnJudgement(queryCenter,k));

		// Take the top k

		return tmp.takeOrdered(k, new PolygonDistanceComparator(queryCenter));

	}
	/**
	 * Spatial K Nearest Neighbors query using index
	 * @param objectRDD specify the input rectangelRDD
	 * @param queryCenter specify the query center 
	 * @param k specify the K
	 * @return A list which contains K nearest points
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
