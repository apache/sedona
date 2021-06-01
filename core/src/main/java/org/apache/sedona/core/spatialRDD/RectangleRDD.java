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

import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.formatMapper.RectangleFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;

// TODO: Auto-generated Javadoc

/**
 * The Class RectangleRDD.
 */
@Deprecated
public class RectangleRDD
        extends SpatialRDD<Polygon>
{
    /**
     * Instantiates a new rectangle RDD.
     */
    public RectangleRDD() {
        this(null);
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     */
    public RectangleRDD(JavaRDD<Polygon> rawSpatialRDD)
    {
        super(GeometryType.RECTANGLE);
        this.rawSpatialRDD = rawSpatialRDD;
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public RectangleRDD(JavaRDD<Polygon> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode) {
        super(GeometryType.RECTANGLE);
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
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
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new RectangleFormatMapper(Offset, splitter, carryInputData)));
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
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(new RectangleFormatMapper(Offset, splitter, carryInputData)));
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
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public RectangleRDD(JavaRDD<Polygon> rawSpatialRDD, Envelope datasetBoundary, Integer approximateTotalCount) {
        super(GeometryType.RECTANGLE);
        this.rawSpatialRDD = rawSpatialRDD;
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public RectangleRDD(JavaRDD<Polygon> rawSpatialRDD, String sourceEpsgCRSCode, String targetEpsgCode, Envelope datasetBoundary, Integer approximateTotalCount) {
        super(GeometryType.RECTANGLE);
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
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
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new RectangleFormatMapper(Offset, splitter, carryInputData)));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(new RectangleFormatMapper(Offset, splitter, carryInputData)));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Envelope datasetBoundary, Integer approximateTotalCount) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Envelope datasetBoundary, Integer approximateTotalCount) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, Envelope datasetBoundary, Integer approximateTotalCount) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     */
    public RectangleRDD(JavaRDD<Polygon> rawSpatialRDD, StorageLevel newLevel) {
        super(GeometryType.RECTANGLE);
        this.rawSpatialRDD = rawSpatialRDD;
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
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset,
            FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new RectangleFormatMapper(Offset, splitter, carryInputData)));
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
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset,
            FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(new RectangleFormatMapper(Offset, splitter, carryInputData)));
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
            FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
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
            FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
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
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
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
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public RectangleRDD(JavaRDD<Polygon> rawSpatialRDD, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        super(GeometryType.RECTANGLE);
        this.rawSpatialRDD = rawSpatialRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
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
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset,
            FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
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
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset,
            FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(new RectangleFormatMapper(Offset, splitter, carryInputData)));
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
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
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,
            FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
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
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation,
            FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(new RectangleFormatMapper(splitter, carryInputData)));
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
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
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCode the target epsg code
     */
    public RectangleRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel, String sourceEpsgCRSCode, String targetEpsgCode) {
        super(GeometryType.RECTANGLE);
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCode);
        this.analyze(newLevel);
    }
    
    @Override
    public GeometryType getGeometryType() {
        return GeometryType.CIRCLE;
    }
}
