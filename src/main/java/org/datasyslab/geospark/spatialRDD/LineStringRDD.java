/**
 * FILE: LineStringRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.LineStringRDD.java
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
import org.datasyslab.geospark.formatMapper.LineStringFormatMapper;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

// TODO: Auto-generated Javadoc
/**
 * The Class LineStringRDD.
 */
public class LineStringRDD extends SpatialRDD{
	
	/**
	 * Instantiates a new line string RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 */
	public LineStringRDD(JavaRDD<Polygon> rawSpatialRDD) {
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
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData, Integer partitions) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).map(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    
    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).map(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).map(new LineStringFormatMapper(splitter, carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
    }

    
    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).map(new LineStringFormatMapper(splitter, carryInputData)));
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
    
    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
    public RectangleRDD MinimumBoundingRectangle() {
        JavaRDD<Envelope> rectangleRDD = this.rawSpatialRDD.map(new Function<Object, Envelope>() {
            public Envelope call(Object spatialObject) {
                Envelope MBR = ((Geometry)spatialObject).getEnvelopeInternal();
                return MBR;
            }
        });
        return new RectangleRDD(rectangleRDD);
    }
}
