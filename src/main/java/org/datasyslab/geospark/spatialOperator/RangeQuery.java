/**
 * FILE: RangeQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.RangeQuery.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.rangeJudgement.PointRangeFilter;
import org.datasyslab.geospark.rangeJudgement.PointRangeFilterUsingIndex;
import org.datasyslab.geospark.rangeJudgement.PolygonRangeFilter;
import org.datasyslab.geospark.rangeJudgement.PolygonRangeFilterUsingIndex;
import org.datasyslab.geospark.rangeJudgement.RectangleRangeFilter;
import org.datasyslab.geospark.rangeJudgement.RectangleRangeFilterUsingIndex;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
// TODO: Auto-generated Javadoc

/**
 * The Class RangeQuery.
 */
public class RangeQuery implements Serializable{
	
	//I think this part can be refactored.
	//I think I should use factory method, to return a rangeQuery object?
	//Try template later.
	
	//todo: refactor all this into one rangeQuery

	/**
	 * Spatial range query.
	 *
	 * @param pointRDD the point RDD
	 * @param envelope the envelope
	 * @param condition the condition
	 * @return the point RDD
	 */
	public static PointRDD SpatialRangeQuery(PointRDD pointRDD, Envelope envelope, Integer condition) {
		JavaRDD<Point> result = pointRDD.getRawPointRDD().filter(new PointRangeFilter(envelope, condition));
		return new PointRDD(result);
	}
	
	/**
	 * Spatial range query using index.
	 *
	 * @param pointRDD the point RDD
	 * @param envelope the envelope
	 * @param condition the condition
	 * @return the point RDD
	 */
	public static PointRDD SpatialRangeQueryUsingIndex(PointRDD pointRDD, Envelope envelope, Integer condition) {
        if(pointRDD.indexedRDDNoId == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
        }
		JavaRDD<Point> result = pointRDD.indexedRDDNoId.mapPartitions(new PointRangeFilterUsingIndex(envelope));
		return new PointRDD(result);
	}
	
	
	/**
	 * Spatial range query.
	 *
	 * @param polygonRDD the polygon RDD
	 * @param envelope the envelope
	 * @param condition the condition
	 * @return the polygon RDD
	 */
//	public static PointRDD SpatialRangeQuery(PointRDD pointRDD, Polygon polygon, Integer condition) {
//		JavaRDD<Point> result = pointRDD.getRawPointRDD().filter(new PointRangeFilter(polygon, condition));
//		return new PointRDD(result);
//	}
	
	/**
	 * Spatial range query on top of PolygonRDD
	 * @param polygonRDD Input PolygonRDD
	 * @param envelope Query window
	 * @param condition 0 means query predicate is "contains", 1 means query predicate is "intersects"
	 * @return A PolygonRDD contains qualified records
	 */
	public static PolygonRDD SpatialRangeQuery(PolygonRDD polygonRDD, Envelope envelope,Integer condition)
	{
		JavaRDD<Polygon> result=polygonRDD.getRawPolygonRDD().filter(new PolygonRangeFilter(envelope,condition));
		return new PolygonRDD(result);
	}
	
	/**
	 * Spatial range query using index.
	 *
	 * @param polygonRDD the polygon RDD
	 * @param envelope the envelope
	 * @param condition the condition
	 * @return the polygon RDD
	 */
	public static PolygonRDD SpatialRangeQueryUsingIndex(PolygonRDD polygonRDD, Envelope envelope, Integer condition)
	{
        if(polygonRDD.indexedRDDNoId == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
        }
		JavaRDD<Polygon> result=polygonRDD.indexedRDDNoId.mapPartitions(new PolygonRangeFilterUsingIndex(envelope));
		return new PolygonRDD(result);
	}
//	/**
//	 * Spatial range query.
//	 *
//	 * @param polygon the polygon
//	 * @param condition the condition
//	 * @return the polygon rdd
//	 */
//	public static PolygonRDD SpatialRangeQuery(PolygonRDD polygonRDD, Polygon polygon,Integer condition)
//	{
//		JavaRDD<Polygon> result=polygonRDD.getRawPolygonRDD().filter(new PolygonRangeFilter(polygon,condition));
//		return new PolygonRDD(result);
//	}
//

	/**
 * Spatial range query.
 *
 * @param rectangleRDD the rectangle RDD
 * @param envelope the envelope
 * @param condition the condition
 * @return the rectangle RDD
 */
	public static RectangleRDD SpatialRangeQuery(RectangleRDD rectangleRDD, Envelope envelope,Integer condition)
	{
		JavaRDD<Envelope> result= rectangleRDD.getRawRectangleRDD().filter(new RectangleRangeFilter(envelope,condition));
		return new RectangleRDD(result);
	}
	
	/**
	 * Spatial range query using index.
	 *
	 * @param rectangleRDD the rectangle RDD
	 * @param envelope the envelope
	 * @param condition the condition
	 * @return the rectangle RDD
	 */
	public static RectangleRDD SpatialRangeQueryUsingIndex(RectangleRDD rectangleRDD, Envelope envelope,Integer condition)
	{
        if(rectangleRDD.indexedRDDNoId == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDDNoId is null");
        }
		JavaRDD<Envelope> result= rectangleRDD.indexedRDDNoId.mapPartitions(new RectangleRangeFilterUsingIndex(envelope));
		return new RectangleRDD(result);
	}
	
//	/**
//	 * Spatial range query.
//	 *
//	 * @param polygon the polygon
//	 * @param condition the condition
//	 * @return the rectangle rdd
//	 */
//	public static RectangleRDD SpatialRangeQuery(RectangleRDD rectangleRDD, Polygon polygon,Integer condition)
//	{
//		JavaRDD<Envelope> result=rectangleRDD.getRawRectangleRDD().filter(new RectangleRangeFilter(polygon,condition));
//		return new RectangleRDD(result);
//	}
}
