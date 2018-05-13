/*
 * FILE: Point3DRDD
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
package org.datasyslab.geospark.SpatioTemporalRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.formatMapper.Point3DFormatMapper;

// TODO: Auto-generated Javadoc

/**
 * The Class Point3DRDD.
 */

public class Point3DRDD
        extends SpatioTemporialRDD<Point3D>
{

    /**
     * Instantiates a new point RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     */
    public Point3DRDD(JavaRDD<Point3D> rawSpatialRDD)
    {
        this.rawSpatioTemporalRDD = rawSpatialRDD;
    }


    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new Point3DFormatMapper(Offset, Offset, splitter, carryInputData)));
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(new Point3DFormatMapper(Offset, Offset, splitter, carryInputData)));
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new Point3DFormatMapper(2, 2, splitter, carryInputData)));
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(new Point3DFormatMapper(0, 0, splitter, carryInputData)));
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public Point3DRDD(JavaRDD<Point3D> rawSpatialRDD, Cube datasetBoundary, Integer approximateTotalCount)
    {
        this.rawSpatioTemporalRDD = rawSpatialRDD;
        this.boundaryCube = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }


    /**
     * Instantiates a new point3D RDD.
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
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Cube datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new Point3DFormatMapper(Offset, Offset, splitter, carryInputData)));
        this.boundaryCube = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Cube datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(new Point3DFormatMapper(Offset, Offset, splitter, carryInputData)));
        this.boundaryCube = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, Cube datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new Point3DFormatMapper(0, 0, splitter, carryInputData)));
        this.boundaryCube = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Cube datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(new Point3DFormatMapper(0, 0, splitter, carryInputData)));
        this.boundaryCube = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, Cube datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
        this.boundaryCube = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param datasetBoundary the dataset boundary
     * @param approximateTotalCount the approximate total count
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, Cube datasetBoundary, Integer approximateTotalCount)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.boundaryCube = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param rawSpatioTemporalRDD the raw spatial Temporal RDD
     * @param newLevel the new level
     */
    public Point3DRDD(JavaRDD<Point3D> rawSpatioTemporalRDD, StorageLevel newLevel)
    {
        this.rawSpatioTemporalRDD = rawSpatioTemporalRDD;
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new Point3DFormatMapper(Offset, Offset, splitter, carryInputData)));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer Offset, FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(new Point3DFormatMapper(Offset, Offset, splitter, carryInputData)));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(new Point3DFormatMapper(0, 0, splitter, carryInputData)));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(new Point3DFormatMapper(0, 0, splitter, carryInputData)));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new point3D RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
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
    public Point3DRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel)
    {
        this.setRawSpatioTemporalRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
        this.analyze(newLevel);
    }



}
