/**
 * FILE: RangeQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.RangeQuery.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;

import com.vividsolutions.jts.geom.*;
import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.rangeJudgement.RangeFilter;
import org.datasyslab.geospark.rangeJudgement.RangeFilterUsingIndex;
import org.datasyslab.geospark.spatialRDD.*;
import org.datasyslab.geospark.utils.CRSTransformation;

// TODO: Auto-generated Javadoc
/**
 * The Class RangeQuery.
 */
public class RangeQuery implements Serializable{

	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param originalQueryGeometry the original query window
	 * @param considerBoundaryIntersection the consider boundary intersection
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(SpatialRDD<T> spatialRDD, U originalQueryGeometry, boolean considerBoundaryIntersection, boolean useIndex) throws Exception
	{
		U queryGeometry = originalQueryGeometry;
		if(spatialRDD.getCRStransformation())
		{
			queryGeometry = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(),spatialRDD.getTargetEpgsgCode(), originalQueryGeometry);
		}

		if(useIndex==true)
		{
			if(spatialRDD.indexedRawRDD == null) {
				throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
			}
			return spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryGeometry,considerBoundaryIntersection));
		}
		else{
			return spatialRDD.getRawSpatialRDD().filter(new RangeFilter(queryGeometry, considerBoundaryIntersection));
		}
	}

	/**
	 * Spatial range query.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryWindow the original query window
	 * @param considerBoundaryIntersection the consider boundary intersection
	 * @param useIndex the use index
	 * @return the java RDD
	 * @throws Exception the exception
	 */
	public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(SpatialRDD<T> spatialRDD, Envelope queryWindow, boolean considerBoundaryIntersection, boolean useIndex) throws Exception
	{
		Coordinate[] coordinates = new Coordinate[5];
		coordinates[0]=new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
		coordinates[1]=new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
		coordinates[2]=new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
		coordinates[3]=new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
		coordinates[4]=coordinates[0];
		GeometryFactory geometryFactory = new GeometryFactory();
		U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
		return SpatialRangeQuery(spatialRDD, queryGeometry, considerBoundaryIntersection, useIndex);
	}
}
