package org.datasyslab.geospark.spatialOperator;

import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.rangeFilter.PointRangeFilter;
import org.datasyslab.geospark.rangeFilter.PolygonRangeFilter;
import org.datasyslab.geospark.rangeFilter.RectangleRangeFilter;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class RangeQuery {
	
	//I think this part can be refactored.
	
	/**
	 * Spatial range query.
	 * @param pointRDD
	 * 			the point RDD.
	 * @param envelope
	 *            the envelope
	 * @param condition
	 *            the condition
	 * @return the point rdd
	 */
	public PointRDD SpatialRangeQuery(PointRDD pointRDD, Envelope envelope, Integer condition) {
		JavaRDD<Point> result = pointRDD.getPointRDD().filter(new PointRangeFilter(envelope, condition));
		return new PointRDD(result);
	}
	
	/**
	 * Spatial range query for point.
	 *
	 * @param polygon
	 *            the polygon
	 * @param condition
	 *            the condition
	 * @return the point rdd
	 */
	public PointRDD SpatialRangeQuery(PointRDD pointRDD, Polygon polygon, Integer condition) {
		JavaRDD<Point> result = pointRDD.getPointRDD().filter(new PointRangeFilter(polygon, condition));
		return new PointRDD(result);
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param envelope the envelope
	 * @param condition the condition
	 * @return the polygon rdd
	 */
	public PolygonRDD SpatialRangeQuery(PolygonRDD polygonRDD, Envelope envelope,Integer condition)
	{
		JavaRDD<Polygon> result=polygonRDD.getPolygonRDD().filter(new PolygonRangeFilter(envelope,condition));
		return new PolygonRDD(result);
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param polygon the polygon
	 * @param condition the condition
	 * @return the polygon rdd
	 */
	public PolygonRDD SpatialRangeQuery(PolygonRDD polygonRDD, Polygon polygon,Integer condition)
	{
		JavaRDD<Polygon> result=polygonRDD.getPolygonRDD().filter(new PolygonRangeFilter(polygon,condition));
		return new PolygonRDD(result);
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param envelope the envelope
	 * @param condition the condition
	 * @return the rectangle rdd
	 */
	public RectangleRDD SpatialRangeQuery(RectangleRDD rectangleRDD, Envelope envelope,Integer condition)
	{
		JavaRDD<Envelope> result= rectangleRDD.getRectangleRDD().filter(new RectangleRangeFilter(envelope,condition));
		return new RectangleRDD(result);
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param polygon the polygon
	 * @param condition the condition
	 * @return the rectangle rdd
	 */
	public RectangleRDD SpatialRangeQuery(RectangleRDD rectangleRDD, Polygon polygon,Integer condition)
	{
		JavaRDD<Envelope> result=rectangleRDD.getRectangleRDD().filter(new RectangleRangeFilter(polygon,condition));
		return new RectangleRDD(result);
	}
}
