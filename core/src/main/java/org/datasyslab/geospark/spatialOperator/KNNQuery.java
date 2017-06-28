/**
 * FILE: KNNQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.KNNQuery.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.knnJudgement.KnnJudgementUsingIndex;
import org.datasyslab.geospark.knnJudgement.GeometryDistanceComparator;
import org.datasyslab.geospark.knnJudgement.KnnJudgement;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.utils.CRSTransformation;

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
	 * @param originalQueryWindow the original query window
	 * @param k the k
	 * @param useIndex the use index
	 * @return the list
	 */
	public static List<Point> SpatialKnnQuery(PointRDD spatialRDD, Point originalQueryWindow, Integer k, boolean useIndex) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")
		Point queryCenter = originalQueryWindow;
		if(spatialRDD.getCRStransformation())
		{
			queryCenter = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(),spatialRDD.getTargetEpgsgCode(), originalQueryWindow);
		}
		
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
			JavaRDD<Object> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new KnnJudgement(queryCenter,k));
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
	 * @param originalQueryWindow the original query window
	 * @param k the k
	 * @param useIndex the use index
	 * @return the list
	 */
	public static List<Polygon> SpatialKnnQuery(RectangleRDD spatialRDD, Point originalQueryWindow, Integer k, boolean useIndex) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")
		Point queryCenter = originalQueryWindow;
		if(spatialRDD.getCRStransformation())
		{
			queryCenter = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(),spatialRDD.getTargetEpgsgCode(), originalQueryWindow);
		}
		
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
			JavaRDD<Object> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new KnnJudgement(queryCenter,k));
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
	 * @param originalQueryWindow the original query window
	 * @param k the k
	 * @param useIndex the use index
	 * @return the list
	 */
	public static List<Polygon> SpatialKnnQuery(PolygonRDD spatialRDD, Point originalQueryWindow, Integer k, boolean useIndex) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")
		Point queryCenter = originalQueryWindow;
		if(spatialRDD.getCRStransformation())
		{
			queryCenter = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(),spatialRDD.getTargetEpgsgCode(), originalQueryWindow);
		}
		
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
			JavaRDD<Object> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new KnnJudgement(queryCenter,k));
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
	 * @param originalQueryWindow the original query window
	 * @param k the k
	 * @param useIndex the use index
	 * @return the list
	 */
	public static List<LineString> SpatialKnnQuery(LineStringRDD spatialRDD, Point originalQueryWindow, Integer k, boolean useIndex) {
		// For each partation, build a priority queue that holds the topk
		//@SuppressWarnings("serial")
		Point queryCenter = originalQueryWindow;
		if(spatialRDD.getCRStransformation())
		{
			queryCenter = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(),spatialRDD.getTargetEpgsgCode(), originalQueryWindow);
		}
		
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
			JavaRDD<Object> tmp = spatialRDD.getRawSpatialRDD().mapPartitions(new KnnJudgement(queryCenter,k));
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
