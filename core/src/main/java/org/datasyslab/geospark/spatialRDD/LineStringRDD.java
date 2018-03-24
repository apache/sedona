/**
 * FILE: LineStringRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.LineStringRDD.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.LineString;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.formatMapper.LineStringFormatMapper;

// TODO: Auto-generated Javadoc
/**
 * The Class LineStringRDD.
 */
public class LineStringRDD extends SpatialRDD<LineString> {
	
	/**
	 * Instantiates a new line string RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 */
	public LineStringRDD(JavaRDD<LineString> rawSpatialRDD) {
        this.rawSpatialRDD = rawSpatialRDD;
    }
	
	/**
	 * Instantiates a new line string RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 * @param sourceEpsgCRSCode the source epsg CRS code
	 * @param targetEpsgCode the target epsg code
	 */
	public LineStringRDD(JavaRDD<LineString> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode) {
		this.rawSpatialRDD = rawSpatialRDD;
		this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
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
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).mapPartitions(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
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
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).mapPartitions(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
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
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).mapPartitions(new LineStringFormatMapper(splitter, carryInputData)));
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
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).mapPartitions(new LineStringFormatMapper(splitter, carryInputData)));
    }
    
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     */
    public LineStringRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
    }
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     */
    public LineStringRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
    }


    /**
     * Instantiates a new line string RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public LineStringRDD(JavaRDD<LineString> rawSpatialRDD, Envelope datasetBoundary, Integer approximateTotalCount) {
        this.rawSpatialRDD = rawSpatialRDD;
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new line string RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public LineStringRDD(JavaRDD<LineString> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode, Envelope datasetBoundary, Integer approximateTotalCount) {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
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
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).mapPartitions(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
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
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).mapPartitions(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).mapPartitions(new LineStringFormatMapper(splitter, carryInputData)));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }


    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).mapPartitions(new LineStringFormatMapper(splitter, carryInputData)));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }


    /**
     * Instantiates a new line string RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public LineStringRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new line string RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public LineStringRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }



	/**
	 * Instantiates a new line string RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 * @param newLevel the new level
	 */
	public LineStringRDD(JavaRDD<LineString> rawSpatialRDD, StorageLevel newLevel) {
		this.rawSpatialRDD = rawSpatialRDD;
		this.analyze(newLevel);
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
     * @param newLevel the new level
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, Integer startOffset, Integer endOffset,
    		FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).mapPartitions(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        this.analyze(newLevel);
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
     * @param newLevel the new level
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, Integer startOffset, Integer endOffset,
    		FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).mapPartitions(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        this.analyze(newLevel);
    }
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).mapPartitions(new LineStringFormatMapper(splitter, carryInputData)));
        this.analyze(newLevel);
    }

    
    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation,
    		FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).mapPartitions(new LineStringFormatMapper(splitter, carryInputData)));
        this.analyze(newLevel);
    }
    
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public LineStringRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public LineStringRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }
    
    
    
	/**
	 * Instantiates a new line string RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 * @param newLevel the new level
	 * @param sourceEpsgCRSCode the source epsg CRS code
	 * @param targetEpsgCode the target epsg code
	 */
	public LineStringRDD(JavaRDD<LineString> rawSpatialRDD, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
		this.rawSpatialRDD = rawSpatialRDD;
		this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
		this.analyze(newLevel);
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
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, Integer startOffset, Integer endOffset,
    		FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).mapPartitions(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
		this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
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
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, Integer startOffset, Integer endOffset,
    		FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).mapPartitions(new LineStringFormatMapper(startOffset, endOffset, splitter, carryInputData)));
		this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation, partitions).mapPartitions(new LineStringFormatMapper(splitter, carryInputData)));
		this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }

    
    /**
     * Instantiates a new line string RDD.
     *
     * @param SparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public LineStringRDD(JavaSparkContext SparkContext, String InputLocation,
    		FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        this.setRawSpatialRDD(SparkContext.textFile(InputLocation).mapPartitions(new LineStringFormatMapper(splitter, carryInputData)));
		this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }
    
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public LineStringRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
		this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }
    
    /**
     * Instantiates a new line string RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public LineStringRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
		this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }

}
