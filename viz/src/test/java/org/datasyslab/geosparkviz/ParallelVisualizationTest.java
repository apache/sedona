/*
 * FILE: ParallelVisualizationTest
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
package org.datasyslab.geosparkviz;

import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geosparkviz.core.ImageGenerator;
import org.datasyslab.geosparkviz.core.ImageStitcher;
import org.datasyslab.geosparkviz.extension.visualizationEffect.HeatMap;
import org.datasyslab.geosparkviz.utils.ImageType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// TODO: Auto-generated Javadoc

/**
 * The Class ParallelVisualizationTest.
 */
public class ParallelVisualizationTest
        extends GeoSparkVizTestBase
{

    /**
     * The resolution X.
     */
    static int resolutionX;

    /**
     * The resolution Y.
     */
    static int resolutionY;

    /**
     * The partition X.
     */
    static int partitionX;

    /**
     * The partition Y.
     */
    static int partitionY;

    /**
     * Sets the up before class.
     *
     * @throws Exception the exception
     */
    @BeforeClass
    public static void setUpBeforeClass()
            throws Exception
    {
        initialize(ParallelVisualizationTest.class.getSimpleName());
        resolutionX = 1000;

        resolutionY = 600;

        partitionX = 2;

        partitionY = 2;
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @AfterClass
    public static void tearDown()
            throws Exception
    {
        sparkContext.stop();
    }

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
