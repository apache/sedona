/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.spatialRDD;

import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.core.formatMapper.FormatMapper;
import org.apache.sedona.core.formatMapper.PointFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

// TODO: Auto-generated Javadoc

/**
 * The Class PointRDD.
 */

public class PointRDD
        extends SpatialRDD<Point>
{

    /**
     * Instantiates a new point RDD.
     */
    public PointRDD() {}

    /**
     * Instantiates a new point RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     */
    public PointRDD(JavaRDD<Point> rawSpatialRDD)
    {
        this.rawSpatialRDD = rawSpatialRDD;
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public PointRDD(JavaRDD<Point> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
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
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, partitions, null, null, null);
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
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, null, null, null, null);
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
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, partitions, null, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, null, null, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public PointRDD(JavaRDD<Point> rawSpatialRDD, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public PointRDD(JavaRDD<Point> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
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
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, partitions, null, null, null);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, null, null, null, null);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, partitions, null, null, null);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, null, null, null, null);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     */
    public PointRDD(JavaRDD<Point> rawSpatialRDD, StorageLevel newLevel)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.analyze(newLevel);
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
     * @param newLevel the new level
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, partitions, newLevel, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, null, newLevel, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, partitions, newLevel, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, null, newLevel, null, null);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel)
    {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public PointRDD(JavaRDD<Point> rawSpatialRDD, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
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
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter,
            boolean carryInputData, Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        JavaRDD rawTextRDD = partitions != null ? sparkContext.textFile(InputLocation, partitions) : sparkContext.textFile(InputLocation);
        if (Offset != null) {this.setRawSpatialRDD(rawTextRDD.mapPartitions(new PointFormatMapper(Offset, splitter, carryInputData)));}
        else {this.setRawSpatialRDD(rawTextRDD.mapPartitions(new PointFormatMapper(splitter, carryInputData)));}
        if (sourceEpsgCRSCode != null && targetEpsgCode != null) { this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);}
        if (newLevel != null) { this.analyze(newLevel);}
        if (splitter.equals(FileDataSplitter.GEOJSON)) { this.fieldNames = FormatMapper.readGeoJsonPropertyNames(rawTextRDD.take(1).get(0).toString()); }
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter,
            boolean carryInputData, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, Offset, splitter, carryInputData, null, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData,
            Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, partitions, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData,
            StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, splitter, carryInputData, null, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper,
            StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, null, false, partitions, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }

    /**
     * Instantiates a new point RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public PointRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode)
    {
        this(sparkContext, InputLocation, null, null, false, null, newLevel, sourceEpsgCRSCode, targetEpsgCode);
    }
}
