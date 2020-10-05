/*
 * FILE: ScatterPlot
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geosparkviz.extension.visualizationEffect;

import org.locationtech.jts.geom.Envelope;
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
