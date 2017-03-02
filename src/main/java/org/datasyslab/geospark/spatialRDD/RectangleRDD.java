/**
 * FILE: RectangleRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.RectangleRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;

import org.datasyslab.geospark.formatMapper.RectangleFormatMapper;

import com.vividsolutions.jts.geom.Envelope;

/**
 * The Class RectangleRDD.
 */

public class RectangleRDD extends SpatialRDD {

    
    
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 * @deprecated Please append RDD Storage Level after all the existing parameters
	 */
	@Deprecated
	public RectangleRDD(JavaRDD<Envelope> rawSpatialRDD)
	{
		this.rawSpatialRDD = rawSpatialRDD.map(new Function<Envelope,Object>()
		{

			@Override
			public Object call(Envelope spatialObject) throws Exception {
				return spatialObject;
			}
			
		});
        this.analyze();

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
	 * @deprecated Please append RDD Storage Level after all the existing parameters
	 */
	@Deprecated
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,Integer Offset,FileDataSplitter splitter, boolean carryInputData,Integer partitions)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation,partitions).flatMap(new RectangleFormatMapper(Offset,Offset,splitter,carryInputData)));
        this.analyze();
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 * @deprecated Please append RDD Storage Level after all the existing parameters
	 */
	@Deprecated
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,Integer Offset,FileDataSplitter splitter, boolean carryInputData)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(new RectangleFormatMapper(Offset,Offset,splitter,carryInputData)));
        this.analyze();
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 * @param partitions the partitions
	 * @deprecated Please append RDD Storage Level after all the existing parameters
	 */
	@Deprecated
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,FileDataSplitter splitter, boolean carryInputData,Integer partitions)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation,partitions).flatMap(new RectangleFormatMapper(0,0,splitter,carryInputData)));
        this.analyze();
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 * @deprecated Please append RDD Storage Level after all the existing parameters
	 */
	@Deprecated
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,FileDataSplitter splitter, boolean carryInputData)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(new RectangleFormatMapper(0,0,splitter,carryInputData)));
        this.analyze();
	}
	
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).flatMap(userSuppliedMapper));
        this.analyze();
    }
    
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(userSuppliedMapper));
        this.analyze();
    }
	    
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     */
	public RectangleRDD(JavaRDD<Envelope> rawSpatialRDD, StorageLevel newLevel)
	{
		this.rawSpatialRDD = rawSpatialRDD.map(new Function<Envelope,Object>()
		{

			@Override
			public Object call(Envelope spatialObject) throws Exception {
				return spatialObject;
			}
			
		});
        this.analyze(newLevel);

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
	 * @param newLevel the new level
	 */
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,Integer Offset,
			FileDataSplitter splitter, boolean carryInputData,Integer partitions, StorageLevel newLevel)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation,partitions).flatMap(new RectangleFormatMapper(Offset,Offset,splitter,carryInputData)));
        this.analyze(newLevel);
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 * @param newLevel the new level
	 */
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,Integer Offset,
			FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(new RectangleFormatMapper(Offset,Offset,splitter,carryInputData)));
        this.analyze(newLevel);
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 * @param partitions the partitions
	 * @param newLevel the new level
	 */
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,
			FileDataSplitter splitter, boolean carryInputData,Integer partitions, StorageLevel newLevel)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation,partitions).flatMap(new RectangleFormatMapper(0,0,splitter,carryInputData)));
        this.analyze(newLevel);
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param sparkContext the spark context
	 * @param InputLocation the input location
	 * @param splitter the splitter
	 * @param carryInputData the carry input data
	 * @param newLevel the new level
	 */
	public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,
			FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
	{
		this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(new RectangleFormatMapper(0,0,splitter,carryInputData)));
        this.analyze(newLevel);
	}
	
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).flatMap(userSuppliedMapper));
        this.analyze(newLevel);
    }
    
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(userSuppliedMapper));
        this.analyze(newLevel);
    }
}
