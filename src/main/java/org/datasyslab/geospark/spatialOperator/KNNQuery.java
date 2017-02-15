/**
 * FILE: KNNQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.KNNQuery.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.knnJudgement.KnnJudgementUsingIndex;
import org.datasyslab.geospark.knnJudgement.GeometryDistanceComparator;
import org.datasyslab.geospark.knnJudgement.GeometryKnnJudgement;
import org.datasyslab.geospark.knnJudgement.RectangleDistanceComparator;
import org.datasyslab.geospark.knnJudgement.RectangleKnnJudgement;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
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
	 * @param spatialRDD the spatial RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @param useIndex the use index
	 * @return the list
	 */
	public static List<Point> SpatialKnnQuery(PointRDD spatialRDD, Point queryCenter, Integer k, boolean useIndex) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")
		if(useIndex)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
	        }
			JavaRDD<Object> tmp = spatialRDD.indexedRawRDD.mapPartitions(new KnnJudgementUsingIndex(queryCenter,k));
			List<Object> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter,true));
			List<Point> geometryResult = new ArrayList<Point>();
			for(Object spatialObject:result)
			{
				geometryResult.add((Point)spatialObject);
			}
			// Take the top k
			return geometryResult;
		}
		else
		{
			JavaRDD<Object> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new GeometryKnnJudgement(queryCenter,k));
			List<Object> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter,true));
			List<Point> geometryResult = new ArrayList<Point>();
			for(Object spatialObject:result)
			{
				geometryResult.add((Point)spatialObject);
			}
			// Take the top k
			return geometryResult;
		}
	}

	
	/**
	 * Spatial knn query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @param useIndex the use index
	 * @return the list
	 */
	public static List<Envelope> SpatialKnnQuery(RectangleRDD spatialRDD, Point queryCenter, Integer k, boolean useIndex) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")
		if(useIndex)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
	        }
			JavaRDD<Object> tmp = spatialRDD.indexedRawRDD.mapPartitions(new KnnJudgementUsingIndex(queryCenter,k));
			JavaRDD<Envelope> result = tmp.map(new Function<Object, Envelope>()
			{

				@Override
				public Envelope call(Object spatialObject) throws Exception {
					Envelope returnSpatialObject = ((Geometry)spatialObject).getEnvelopeInternal();
					if( ((Geometry)spatialObject).getUserData()!=null)
					{
						returnSpatialObject.setUserData(((Geometry)spatialObject).getUserData());
					}
					return returnSpatialObject;
				}
				
			});
			// Take the top k
			return result.takeOrdered(k, new RectangleDistanceComparator(queryCenter, true));
		}
		else
		{
			JavaRDD<Object> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new RectangleKnnJudgement(queryCenter,k));
			JavaRDD<Envelope> result = tmp.map(new Function<Object,Envelope>()
			{

				@Override
				public Envelope call(Object spatialObject) throws Exception {
					return (Envelope) spatialObject;
				}
			});
			// Take the top k
			return result.takeOrdered(k, new RectangleDistanceComparator(queryCenter, true));
		}
	}

	/**
	 * Spatial knn query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @param useIndex the use index
	 * @return the list
	 */
	public static List<Polygon> SpatialKnnQuery(PolygonRDD spatialRDD, Point queryCenter, Integer k, boolean useIndex) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")
		if(useIndex)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
	        }
			JavaRDD<Object> tmp = spatialRDD.indexedRawRDD.mapPartitions(new KnnJudgementUsingIndex(queryCenter,k));
			List<Object> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter,true));
			List<Polygon> geometryResult = new ArrayList<Polygon>();
			for(Object spatialObject:result)
			{
				geometryResult.add((Polygon)spatialObject);
			}
			// Take the top k
			return geometryResult;
		}
		else
		{
			JavaRDD<Object> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new GeometryKnnJudgement(queryCenter,k));
			List<Object> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter,true));
			List<Polygon> geometryResult = new ArrayList<Polygon>();
			for(Object spatialObject:result)
			{
				geometryResult.add((Polygon)spatialObject);
			}
			// Take the top k
			return geometryResult;
		}
	}
	
	/**
	 * Spatial knn query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryCenter the query center
	 * @param k the k
	 * @param useIndex the use index
	 * @return the list
	 */
	public static List<LineString> SpatialKnnQuery(LineStringRDD spatialRDD, Point queryCenter, Integer k, boolean useIndex) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")
		if(useIndex)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
	        }
			JavaRDD<Object> tmp = spatialRDD.indexedRawRDD.mapPartitions(new KnnJudgementUsingIndex(queryCenter,k));
			List<Object> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter,true));
			List<LineString> geometryResult = new ArrayList<LineString>();
			for(Object spatialObject:result)
			{
				geometryResult.add((LineString)spatialObject);
			}
			// Take the top k
			return geometryResult;
		}
		else
		{
			JavaRDD<Object> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new GeometryKnnJudgement(queryCenter,k));
			List<Object> result = tmp.takeOrdered(k, new GeometryDistanceComparator(queryCenter,true));
			List<LineString> geometryResult = new ArrayList<LineString>();
			for(Object spatialObject:result)
			{
				geometryResult.add((LineString)spatialObject);
			}
			// Take the top k
			return geometryResult;
		}
	}
}
