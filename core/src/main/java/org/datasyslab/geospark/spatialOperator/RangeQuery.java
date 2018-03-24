/*
 * FILE: RangeQuery
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.rangeJudgement.RangeFilter;
import org.datasyslab.geospark.rangeJudgement.RangeFilterUsingIndex;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geospark.utils.CRSTransformation;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Class RangeQuery.
 */
public class RangeQuery
        implements Serializable
{

    /**
     * Spatial range query. Return objects in SpatialRDD are covered/intersected by originalQueryGeometry
     *
     * @param spatialRDD the spatial RDD
     * @param originalQueryGeometry the original query window
     * @param considerBoundaryIntersection the consider boundary intersection
     * @param useIndex the use index
     * @return the java RDD
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(SpatialRDD<T> spatialRDD, U originalQueryGeometry, boolean considerBoundaryIntersection, boolean useIndex)
            throws Exception
    {
        U queryGeometry = originalQueryGeometry;
        if (spatialRDD.getCRStransformation()) {
            queryGeometry = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(), spatialRDD.getTargetEpgsgCode(), originalQueryGeometry);
        }

        if (useIndex == true) {
            if (spatialRDD.indexedRawRDD == null) {
                throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
            }
            return spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryGeometry, considerBoundaryIntersection, true));
        }
        else {
            return spatialRDD.getRawSpatialRDD().filter(new RangeFilter(queryGeometry, considerBoundaryIntersection, true));
        }
    }

    /**
     * Spatial range query. Return objects in SpatialRDD are covered/intersected by queryWindow/Envelope
     *
     * @param spatialRDD the spatial RDD
     * @param queryWindow the original query window
     * @param considerBoundaryIntersection the consider boundary intersection
     * @param useIndex the use index
     * @return the java RDD
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(SpatialRDD<T> spatialRDD, Envelope queryWindow, boolean considerBoundaryIntersection, boolean useIndex)
            throws Exception
    {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
        coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
        coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
        coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
        coordinates[4] = coordinates[0];
        GeometryFactory geometryFactory = new GeometryFactory();
        U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
        return SpatialRangeQuery(spatialRDD, queryGeometry, considerBoundaryIntersection, useIndex);
    }

    /**
     * Spatial range query. Return objects in SpatialRDD cover/intersect by queryWindow/Envelope
     *
     * @param spatialRDD the spatial RDD
     * @param queryWindow the original query window
     * @param considerBoundaryIntersection the consider boundary intersection
     * @param useIndex the use index
     * @return the java RDD
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(Envelope queryWindow, SpatialRDD<T> spatialRDD, boolean considerBoundaryIntersection, boolean useIndex)
            throws Exception
    {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
        coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
        coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
        coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
        coordinates[4] = coordinates[0];
        GeometryFactory geometryFactory = new GeometryFactory();
        U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
        return SpatialRangeQuery(queryGeometry, spatialRDD, considerBoundaryIntersection, useIndex);
    }

    /**
     * Spatial range query. Return objects in SpatialRDD cover/intersect originalQueryGeometry
     *
     * @param spatialRDD the spatial RDD
     * @param originalQueryGeometry the original query window
     * @param considerBoundaryIntersection the consider boundary intersection
     * @param useIndex the use index
     * @return the java RDD
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(U originalQueryGeometry, SpatialRDD<T> spatialRDD, boolean considerBoundaryIntersection, boolean useIndex)
            throws Exception
    {
        U queryGeometry = originalQueryGeometry;
        if (spatialRDD.getCRStransformation()) {
            queryGeometry = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(), spatialRDD.getTargetEpgsgCode(), originalQueryGeometry);
        }

        if (useIndex == true) {
            if (spatialRDD.indexedRawRDD == null) {
                throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
            }
            return spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryGeometry, considerBoundaryIntersection, false));
        }
        else {
            return spatialRDD.getRawSpatialRDD().filter(new RangeFilter(queryGeometry, considerBoundaryIntersection, false));
        }
    }
}
