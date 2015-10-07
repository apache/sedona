package org.datasyslab.geospark.spatialOperator;

import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.rangeFilter.PointRangeFilter;
import org.datasyslab.geospark.rangeFilter.PolygonRangeFilter;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

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
	public PolygonRDD SpatialRangeQuery(Envelope envelope,Integer condition)
	{
		JavaRDD<Polygon> result=this.polygonRDD.filter(new PolygonRangeFilter(envelope,condition));
		return new PolygonRDD(result);
	}
	
	/**
	 * Spatial range query.
	 *
	 * @param polygon the polygon
	 * @param condition the condition
	 * @return the polygon rdd
	 */
	public PolygonRDD SpatialRangeQuery(Polygon polygon,Integer condition)
	{
		JavaRDD<Polygon> result=this.polygonRDD.filter(new PolygonRangeFilter(polygon,condition));
		return new PolygonRDD(result);
	}
}
