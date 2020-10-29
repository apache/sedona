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
package org.apache.sedona.viz;

import org.apache.sedona.core.spatialRDD.LineStringRDD;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.RectangleRDD;
import org.apache.sedona.viz.core.ImageGenerator;
import org.apache.sedona.viz.core.ImageStitcher;
import org.apache.sedona.viz.extension.visualizationEffect.HeatMap;
import org.apache.sedona.viz.utils.ImageType;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

// TODO: Auto-generated Javadoc

/**
 * The Class ParallelVisualizationTest.
 */
public class ParallelVisualizationTest
        extends VizTestBase
{

    /**
     * The resolution X.
     */
    static int resolutionX = 1000;

    /**
     * The resolution Y.
     */
    static int resolutionY = 600;

    /**
     * The partition X.
     */
    static int partitionX = 2;

    /**
     * The partition Y.
     */
    static int partitionY = 2;

    /**
     * Test point RDD visualization.
     *
     * @throws Exception the exception
     */
    @Test
    public void testPointRDDVisualization()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions, StorageLevel.MEMORY_ONLY());
        HeatMap visualizationOperator = new HeatMap(resolutionX, resolutionY, USMainLandBoundary, false, 2, partitionX, partitionY, true, true);
        visualizationOperator.Visualize(sparkContext, spatialRDD);

        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/PointRDD", ImageType.PNG, 0, partitionX, partitionY);
        ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/PointRDD", resolutionX, resolutionY, 0, partitionX, partitionY);
    }

    /**
     * Test rectangle RDD visualization with tiles.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRectangleRDDVisualizationWithTiles()
            throws Exception
    {
        RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
        HeatMap visualizationOperator = new HeatMap(resolutionX, resolutionY, USMainLandBoundary, false, 2, partitionX, partitionY, true, true);
        visualizationOperator.Visualize(sparkContext, spatialRDD);

        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/RectangleRDDWithTiles", ImageType.PNG, 0, partitionX, partitionY);
        ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/RectangleRDDWithTiles", resolutionX, resolutionY, 0, partitionX, partitionY);
    }

    /**
     * Test rectangle RDD visualization no tiles.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRectangleRDDVisualizationNoTiles()
            throws Exception
    {
        RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
        HeatMap visualizationOperator = new HeatMap(resolutionX, resolutionY, USMainLandBoundary, false, 5, partitionX, partitionY, true, true);
        visualizationOperator.Visualize(sparkContext, spatialRDD);

        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/RectangleRDDNoTiles", ImageType.PNG, 0, partitionX, partitionY);
        ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/RectangleRDDNoTiles", resolutionX, resolutionY, 0, partitionX, partitionY);
    }

    /**
     * Test polygon RDD visualization.
     *
     * @throws Exception the exception
     */
    @Test
    public void testPolygonRDDVisualization()
            throws Exception
    {
        //UserSuppliedPolygonMapper userSuppliedPolygonMapper = new UserSuppliedPolygonMapper();
        PolygonRDD spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY());
        HeatMap visualizationOperator = new HeatMap(resolutionX, resolutionY, USMainLandBoundary, false, 2, partitionX, partitionY, true, true);
        visualizationOperator.Visualize(sparkContext, spatialRDD);

        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/PolygonRDD", ImageType.PNG, 0, partitionX, partitionY);
        ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/PolygonRDD", resolutionX, resolutionY, 0, partitionX, partitionY);
    }

    /**
     * Test line string RDD visualization.
     *
     * @throws Exception the exception
     */
    @Test
    public void testLineStringRDDVisualization()
            throws Exception
    {
        LineStringRDD spatialRDD = new LineStringRDD(sparkContext, LineStringInputLocation, LineStringSplitter, false, LineStringNumPartitions, StorageLevel.MEMORY_ONLY());
        HeatMap visualizationOperator = new HeatMap(resolutionX, resolutionY, USMainLandBoundary, false, 2, partitionX, partitionY, true, true);
        visualizationOperator.Visualize(sparkContext, spatialRDD);

        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/LineStringRDD", ImageType.PNG, 0, partitionX, partitionY);
        ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/LineStringRDD", resolutionX, resolutionY, 0, partitionX, partitionY);
    }
}
