/**
 * FILE: RangeQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.RangeQuery.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.rangeJudgement.GeometryRangeFilter;
import org.datasyslab.geospark.rangeJudgement.RangeFilterUsingIndex;
import org.datasyslab.geospark.rangeJudgement.RectangleRangeFilter;
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
 * The Class RangeQuery.
 */
public class RangeQuery implements Serializable{

	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryWindow the query window
	 * @param condition the condition
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static JavaRDD<Point> SpatialRangeQuery(PointRDD spatialRDD, Envelope queryWindow, Integer condition, boolean useIndex) throws Exception {
		if(useIndex==true)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
	        }
			JavaRDD<Object> result = spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryWindow));
			return result.map(new Function<Object, Point>()
			{

				@Override
				public Point call(Object spatialObject) throws Exception {
					return (Point)spatialObject;
				}
				
			});
		}
		else{
			JavaRDD<Object> result = spatialRDD.getRawSpatialRDD().filter(new GeometryRangeFilter(queryWindow, condition));
			return result.map(new Function<Object,Point>()
			{
				@Override
				public Point call(Object spatialObject) throws Exception {
					return (Point)spatialObject;
				}
				
			});
		}
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryWindow the query window
	 * @param condition the condition
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static JavaRDD<Point> SpatialRangeQuery(PointRDD spatialRDD, Polygon queryWindow, Integer condition, boolean useIndex) throws Exception {
		if(useIndex==true)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
	        }
			JavaRDD<Object> result = spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryWindow));
			return result.map(new Function<Object, Point>()
			{

				@Override
				public Point call(Object spatialObject) throws Exception {
					return (Point)spatialObject;
				}
				
			});
		}
		else{
			JavaRDD<Object> result = spatialRDD.getRawSpatialRDD().filter(new GeometryRangeFilter(queryWindow, condition));
			return result.map(new Function<Object,Point>()
			{
				@Override
				public Point call(Object spatialObject) throws Exception {
					return (Point)spatialObject;
				}
				
			});
		}
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryWindow the query window
	 * @param condition the condition
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static JavaRDD<Polygon> SpatialRangeQuery(PolygonRDD spatialRDD, Envelope queryWindow,Integer condition,boolean useIndex) throws Exception
	{
		if(useIndex==true)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
	        }
			JavaRDD<Object> result = spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryWindow));
			return result.map(new Function<Object, Polygon>()
			{

				@Override
				public Polygon call(Object spatialObject) throws Exception {
					return (Polygon)spatialObject;
				}
				
			});
		}
		else{
			JavaRDD<Object> result = spatialRDD.getRawSpatialRDD().filter(new GeometryRangeFilter(queryWindow, condition));
			return result.map(new Function<Object,Polygon>()
			{
				@Override
				public Polygon call(Object spatialObject) throws Exception {
					return (Polygon)spatialObject;
				}
				
			});
		}
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryWindow the query window
	 * @param condition the condition
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static JavaRDD<Polygon> SpatialRangeQuery(PolygonRDD spatialRDD, Polygon queryWindow,Integer condition,boolean useIndex) throws Exception
	{
		if(useIndex==true)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
	        }
			JavaRDD<Object> result = spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryWindow));
			return result.map(new Function<Object, Polygon>()
			{

				@Override
				public Polygon call(Object spatialObject) throws Exception {
					return (Polygon)spatialObject;
				}
				
			});
		}
		else{
			JavaRDD<Object> result = spatialRDD.getRawSpatialRDD().filter(new GeometryRangeFilter(queryWindow, condition));
			return result.map(new Function<Object,Polygon>()
			{
				@Override
				public Polygon call(Object spatialObject) throws Exception {
					return (Polygon)spatialObject;
				}
				
			});
		}
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryWindow the query window
	 * @param condition the condition
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static JavaRDD<Envelope> SpatialRangeQuery(RectangleRDD spatialRDD, Envelope queryWindow,Integer condition,boolean useIndex) throws Exception
	{
		if(useIndex==true)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
	        }
			JavaRDD<Object> result = spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryWindow));
			return result.map(new Function<Object, Envelope>()
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
		}
		else{
			JavaRDD<Object> result = spatialRDD.getRawSpatialRDD().filter(new RectangleRangeFilter(queryWindow, condition));
			return result.map(new Function<Object,Envelope>()
			{
				@Override
				public Envelope call(Object spatialObject) throws Exception {
					return (Envelope)spatialObject;
				}
				
			});
		}
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryWindow the query window
	 * @param condition the condition
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static JavaRDD<LineString> SpatialRangeQuery(LineStringRDD spatialRDD, Envelope queryWindow,Integer condition,boolean useIndex) throws Exception
	{
		if(useIndex==true)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
	        }
			JavaRDD<Object> result = spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryWindow));
			return result.map(new Function<Object, LineString>()
			{

				@Override
				public LineString call(Object spatialObject) throws Exception {
					return (LineString)spatialObject;
				}
				
			});
		}
		else{
			JavaRDD<Object> result = spatialRDD.getRawSpatialRDD().filter(new GeometryRangeFilter(queryWindow, condition));
			return result.map(new Function<Object,LineString>()
			{
				@Override
				public LineString call(Object spatialObject) throws Exception {
					return (LineString)spatialObject;
				}
				
			});
		}
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryWindow the query window
	 * @param condition the condition
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static JavaRDD<LineString> SpatialRangeQuery(LineStringRDD spatialRDD, Polygon queryWindow,Integer condition,boolean useIndex) throws Exception
	{
		if(useIndex==true)
		{
	        if(spatialRDD.indexedRawRDD == null) {
	            throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
	        }
			JavaRDD<Object> result = spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryWindow));
			return result.map(new Function<Object, LineString>()
			{

				@Override
				public LineString call(Object spatialObject) throws Exception {
					return (LineString)spatialObject;
				}
				
			});
		}
		else{
			JavaRDD<Object> result = spatialRDD.getRawSpatialRDD().filter(new GeometryRangeFilter(queryWindow, condition));
			return result.map(new Function<Object,LineString>()
			{
				@Override
				public LineString call(Object spatialObject) throws Exception {
					return (LineString)spatialObject;
				}
				
			});
		}
	}
}
