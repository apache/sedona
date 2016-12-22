/**
 * FILE: RectangleRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.RectangleRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All right reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.datasyslab.geospark.enums.FileDataSplitter;

import org.datasyslab.geospark.formatMapper.RectangleFormatMapper;

import com.vividsolutions.jts.geom.Envelope;


// TODO: Auto-generated Javadoc

/**
 * The Class RectangleRDD.
 */

public class RectangleRDD extends SpatialRDD {

    
    
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 */
	public RectangleRDD(JavaRDD<Envelope> rawSpatialRDD)
	{
		this.rawSpatialRDD = rawSpatialRDD.map(new Function<Envelope,Object>()
		{

			@Override
			public Object call(Envelope spatialObject) throws Exception {
				return spatialObject;
			}
			
		});
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();

	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 * @param partitions the partitions
	 */
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,Integer Offset,FileDataSplitter splitter, boolean carryInputData,Integer partitions)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,Offset,splitter,carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,Integer Offset,FileDataSplitter splitter, boolean carryInputData)
	{
		//final Integer offset=Offset;
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation).map(new RectangleFormatMapper(Offset,Offset,splitter,carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 * @param partitions the partitions
	 */
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,FileDataSplitter splitter, boolean carryInputData,Integer partitions)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation,partitions).map(new RectangleFormatMapper(0,0,splitter,carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,FileDataSplitter splitter, boolean carryInputData)
	{
		//final Integer offset=Offset;
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation).map(new RectangleFormatMapper(0,0,splitter,carryInputData)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
	}
	
}
