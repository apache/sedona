/**
 * FILE: PolygonRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.PolygonRDD.java
 * Copyright (c) 2016 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import org.datasyslab.geospark.enums.FileDataSplitter;

import org.datasyslab.geospark.formatMapper.PolygonFormatMapper;

import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.PolygonXMaxComparator;
import org.datasyslab.geospark.utils.PolygonXMinComparator;
import org.datasyslab.geospark.utils.PolygonYMaxComparator;
import org.datasyslab.geospark.utils.PolygonYMinComparator;
import org.datasyslab.geospark.utils.RDDSampleUtils;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.precision.GeometryPrecisionReducer;


// TODO: Auto-generated Javadoc

/**
 * The Class PolygonRDD.
 */
public class PolygonRDD extends SpatialRDD {
    
    private Envelope minXEnvelope;
    private Envelope minYEnvelope;
    private Envelope maxXEnvelope;
    private Envelope maxYEnvelope;


    /**
     * Instantiates a new polygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     */
    public PolygonRDD(JavaRDD<Polygon> rawSpatialRDD) {
		this.rawSpatialRDD = rawSpatialRDD.map(new Function<Polygon,Object>()
		{
			@Override
			public Object call(Polygon spatialObject) throws Exception {
				return spatialObject;
			}
			
		});
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param partitions the partitions
     */ 
    public PolygonRDD(JavaSparkContext spark, String InputLocation, Integer Offset, FileDataSplitter splitter, Integer partitions) {
        this.setRawSpatialRDD(spark.textFile(InputLocation, partitions).map(new PolygonFormatMapper(Offset, splitter)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     */
    public PolygonRDD(JavaSparkContext spark, String InputLocation, Integer Offset, FileDataSplitter splitter) {
        this.setRawSpatialRDD(spark.textFile(InputLocation).map(new PolygonFormatMapper(Offset, splitter)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.spatialRDD.SpatialRDD#boundary()
     */
    public Envelope boundary() {
        minXEnvelope = ((Polygon) this.rawSpatialRDD
                .min((PolygonXMinComparator) GeometryComparatorFactory.createComparator("polygon", "x", "min"))).getEnvelopeInternal();
        Double minLongitude = minXEnvelope.getMinX();

        maxXEnvelope = ((Polygon) this.rawSpatialRDD
                .max((PolygonXMaxComparator) GeometryComparatorFactory.createComparator("polygon", "x", "max"))).getEnvelopeInternal();
        Double maxLongitude = maxXEnvelope.getMaxX();

        minYEnvelope = ((Polygon) this.rawSpatialRDD
                .min((PolygonYMinComparator) GeometryComparatorFactory.createComparator("polygon", "y", "min"))).getEnvelopeInternal();
        Double minLatitude = minYEnvelope.getMinY();

        maxYEnvelope = ((Polygon) this.rawSpatialRDD
                .max((PolygonYMaxComparator) GeometryComparatorFactory.createComparator("polygon", "y", "max"))).getEnvelopeInternal();
        Double maxLatitude = maxYEnvelope.getMaxY();
        this.boundary[0] = minLongitude;
        this.boundary[1] = minLatitude;
        this.boundary[2] = maxLongitude;
        this.boundary[3] = maxLatitude;
        this.boundaryEnvelope = new Envelope(boundary[0],boundary[2],boundary[1],boundary[3]);
        return new Envelope(boundary[0], boundary[2], boundary[1], boundary[3]);
    }

    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
    public RectangleRDD MinimumBoundingRectangle() {
        JavaRDD<Envelope> rectangleRDD = this.rawSpatialRDD.map(new Function<Object, Envelope>() {

            public Envelope call(Object spatialObject) {
                Envelope MBR = ((Polygon)spatialObject).getEnvelope().getEnvelopeInternal();//.getEnvelopeInternal();
                return MBR;
            }
        });
        return new RectangleRDD(rectangleRDD);
    }

    /**
     * Polygon union.
     *
     * @return the polygon
     */
    public Polygon PolygonUnion() {
        Object result = this.rawSpatialRDD.reduce(new Function2<Object, Object, Object>() {
            public Polygon call(Object v1, Object v2) {
                //Reduce precision in JTS to avoid TopologyException
                PrecisionModel pModel = new PrecisionModel();
                GeometryPrecisionReducer pReducer = new GeometryPrecisionReducer(pModel);
                Geometry p1 = pReducer.reduce((Polygon) v1);
                Geometry p2 = pReducer.reduce((Polygon) v2);
                //Union two polygons
                Geometry polygonGeom = p1.union(p2);
                Coordinate[] coordinates = polygonGeom.getCoordinates();
                ArrayList<Coordinate> coordinateList = new ArrayList<Coordinate>(Arrays.asList(coordinates));
                Coordinate lastCoordinate = coordinateList.get(0);
                coordinateList.add(lastCoordinate);
                Coordinate[] coordinatesClosed = new Coordinate[coordinateList.size()];
                coordinatesClosed = coordinateList.toArray(coordinatesClosed);
                GeometryFactory fact = new GeometryFactory();
                LinearRing linear = new GeometryFactory().createLinearRing(coordinatesClosed);
                Polygon polygon = new Polygon(linear, null, fact);
                //Return the two polygon union result
                return polygon;
            }
        });
        return (Polygon)result;
    }
}

