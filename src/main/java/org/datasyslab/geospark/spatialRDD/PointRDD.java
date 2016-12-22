/**
 * FILE: PointRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.PointRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All right reserved.
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

import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

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
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).map(new PointFormatMapper(Offset,Offset, splitter, carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public PointRDD (JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).map(new PointFormatMapper(Offset, Offset, splitter, carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).map(new PointFormatMapper(0, 0, splitter, carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public PointRDD (JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).map(new PointFormatMapper(0, 0, splitter, carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }
    
    /**
     * Save as geo JSON.
     *
     * @param outputLocation the output location
     */
    public void saveAsGeoJSON(String outputLocation) {
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<Object>, String>() {
            @Override
            public Iterable<String> call(Iterator<Object> iterator) throws Exception {
                ArrayList<String> result = new ArrayList<String>();
                GeoJSONWriter writer = new GeoJSONWriter();
                while (iterator.hasNext()) {
                	Geometry spatialObject = (Geometry)iterator.next();
                    GeoJSON json = writer.write(spatialObject);
                    String jsonstring = json.toString();
                    result.add(jsonstring);
                }
                return result;
            }
        }).saveAsTextFile(outputLocation);
    }

}
