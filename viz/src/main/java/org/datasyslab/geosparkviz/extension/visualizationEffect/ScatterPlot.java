/*
 * FILE: ScatterPlot
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
package org.datasyslab.geosparkviz.extension.visualizationEffect;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparkviz.core.VisualizationOperator;
import org.datasyslab.geosparkviz.utils.ColorizeOption;

// TODO: Auto-generated Javadoc

/**
 * The Class ScatterPlot.
 */
public class ScatterPlot
        extends VisualizationOperator
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(ScatterPlot.class);

    /**
     * Instantiates a new scatter plot.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     */
    public ScatterPlot(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate)
    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.NORMAL, reverseSpatialCoordinate, -1, -1, false, false, false);
    }

    /**
     * Instantiates a new scatter plot.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param generateVectorImage the generate vector image
     */
    public ScatterPlot(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate, boolean generateVectorImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.NORMAL, reverseSpatialCoordinate, -1, -1, false, false, generateVectorImage);
    }

    /**
     * Instantiates a new scatter plot.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @param parallelRenderImage the parallel render image
     * @param generateVectorImage the generate vector image
     */
    public ScatterPlot(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate,
            int partitionX, int partitionY, boolean parallelRenderImage, boolean generateVectorImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.NORMAL, reverseSpatialCoordinate,
                partitionX, partitionY, false, parallelRenderImage, generateVectorImage);
    }

    /**
     * Instantiates a new scatter plot.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @param parallelRenderImage the parallel render image
     */
    public ScatterPlot(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate,
            int partitionX, int partitionY, boolean parallelRenderImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.NORMAL, reverseSpatialCoordinate,
                partitionX, partitionY, false, parallelRenderImage, false);
    }

    /**
     * Instantiates a new scatter plot.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param colorizeOption the colorize option
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param generateVectorImage the generate vector image
     */
    public ScatterPlot(int resolutionX, int resolutionY, Envelope datasetBoundary, ColorizeOption colorizeOption, boolean reverseSpatialCoordinate, boolean generateVectorImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, colorizeOption, reverseSpatialCoordinate, -1, -1, false, false, generateVectorImage);
    }

    /**
     * Instantiates a new scatter plot.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param colorizeOption the colorize option
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @param parallelRenderImage the parallel render image
     * @param generateVectorImage the generate vector image
     */
    public ScatterPlot(int resolutionX, int resolutionY, Envelope datasetBoundary, ColorizeOption colorizeOption, boolean reverseSpatialCoordinate,
            int partitionX, int partitionY, boolean parallelRenderImage, boolean generateVectorImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, colorizeOption, reverseSpatialCoordinate,
                partitionX, partitionY, false, parallelRenderImage, generateVectorImage);
    }

    /**
     * Instantiates a new scatter plot.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param colorizeOption the colorize option
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @param parallelRenderImage the parallel render image
     */
    public ScatterPlot(int resolutionX, int resolutionY, Envelope datasetBoundary, ColorizeOption colorizeOption, boolean reverseSpatialCoordinate,
            int partitionX, int partitionY, boolean parallelRenderImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, colorizeOption, reverseSpatialCoordinate,
                partitionX, partitionY, false, parallelRenderImage, false);
    }

    /**
     * Visualize.
     *
     * @param sparkContext the spark context
     * @param spatialRDD the spatial RDD
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean Visualize(JavaSparkContext sparkContext, SpatialRDD spatialRDD)
            throws Exception
    {
        logger.info("[GeoSparkViz][Visualize][Start]");
        this.Rasterize(sparkContext, spatialRDD, true);
        this.Colorize();
        this.RenderImage(sparkContext);
        logger.info("[GeoSparkViz][Visualize][Stop]");
        return true;
    }
}
