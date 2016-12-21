/**
 * FILE: PointRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.PointRDD.java
 * Copyright (c) 2016 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.util.ArrayList;
import java.util.Iterator;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import org.datasyslab.geospark.enums.FileDataSplitter;

import org.datasyslab.geospark.formatMapper.PointFormatMapper;

import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.PointXComparator;
import org.datasyslab.geospark.utils.PointYComparator;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;


// TODO: Auto-generated Javadoc
/**
 * The Class PointRDD.
 */

public class PointRDD extends SpatialRDD {
    
	/**
	 * Instantiates a new point RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 */
	public PointRDD(JavaRDD<Point> rawSpatialRDD)
	{
		this.rawSpatialRDD = rawSpatialRDD.map(new Function<Point,Object>()
		{

			@Override
			public Object call(Point spatialObject) throws Exception {
				return spatialObject;
			}
			
		});
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
	}
	
    /**
     * Instantiates a new point RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param partitions the partitions
     */
    public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, FileDataSplitter splitter, Integer partitions) {
        this.setRawSpatialRDD(spark.textFile(InputLocation, partitions).map(new PointFormatMapper(Offset, splitter)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    
    /**
     * Instantiates a new point RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     */
    public PointRDD (JavaSparkContext spark, String InputLocation, Integer Offset, FileDataSplitter splitter) {
        this.setRawSpatialRDD(spark.textFile(InputLocation).map(new PointFormatMapper(Offset, splitter)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.spatialRDD.SpatialRDD#boundary()
     */
    public Envelope boundary() {
        Double minLongitude = ((Point) this.rawSpatialRDD
                .min((PointXComparator) GeometryComparatorFactory.createComparator("point", "x"))).getX();
        Double maxLongitude = ((Point) this.rawSpatialRDD
                .max((PointXComparator) GeometryComparatorFactory.createComparator("point", "x"))).getX();
        Double minLatitude = ((Point) this.rawSpatialRDD
                .min((PointYComparator) GeometryComparatorFactory.createComparator("point", "y"))).getY();
        Double maxLatitude = ((Point) this.rawSpatialRDD
                .max((PointYComparator) GeometryComparatorFactory.createComparator("point", "y"))).getY();
        this.boundary[0] = minLongitude;
        this.boundary[1] = minLatitude;
        this.boundary[2] = maxLongitude;
        this.boundary[3] = maxLatitude;
        this.boundaryEnvelope = new Envelope(minLongitude, maxLongitude, minLatitude, maxLatitude);
        return boundaryEnvelope;
    }

    /**
     * Save as geo JSON.
     *
     * @param outputLocation the output location
     */
    public void saveAsGeoJSON(String outputLocation) {
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<Object>, String>() {
            @Override
            public Iterator<String> call(Iterator<Object> iterator) throws Exception {
                ArrayList<String> result = new ArrayList<String>();
                GeoJSONWriter writer = new GeoJSONWriter();
                while (iterator.hasNext()) {
                	Geometry spatialObject = (Geometry)iterator.next();
                    GeoJSON json = writer.write(spatialObject);
                    String jsonstring = json.toString();
                    result.add(jsonstring);
                }
                return result.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }

}
