/*
 * FILE: HeatMap
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
import org.datasyslab.geosparkviz.extension.photoFilter.GaussianBlur;
import org.datasyslab.geosparkviz.utils.ColorizeOption;

import java.awt.Color;

// TODO: Auto-generated Javadoc

/**
 * The Class HeatMap.
 */
public class HeatMap
        extends VisualizationOperator
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(HeatMap.class);

    /**
     * Instantiates a new heat map.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param blurRadius the blur radius
     */
    public HeatMap(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate, int blurRadius)

    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.SPATIALAGGREGATION, reverseSpatialCoordinate, -1, -1, false, false, false);
        GaussianBlur gaussianBlur = new GaussianBlur(blurRadius);
        this.InitPhotoFilterWeightMatrix(gaussianBlur);
    }

    /**
     * Instantiates a new heat map.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param blurRadius the blur radius
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @param parallelPhotoFilter the parallel photo filter
     * @param parallelRenderImage the parallel render image
     */
    public HeatMap(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate, int blurRadius,
            int partitionX, int partitionY, boolean parallelPhotoFilter, boolean parallelRenderImage)

    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.SPATIALAGGREGATION, reverseSpatialCoordinate,
                partitionX, partitionY, parallelPhotoFilter, parallelRenderImage, false);
        GaussianBlur gaussianBlur = new GaussianBlur(blurRadius);
        this.InitPhotoFilterWeightMatrix(gaussianBlur);
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geosparkviz.core.VisualizationOperator#EncodeToColor(int)
     */
    @Override
    protected Integer EncodeToRGB(int normailizedCount)
            throws Exception
    {
        int alpha = 150;
        Color[] colors = new Color[] {new Color(153, 255, 0, alpha), new Color(204, 255, 0, alpha), new Color(255, 255, 0, alpha),
                new Color(255, 204, 0, alpha), new Color(255, 153, 0, alpha), new Color(255, 102, 0, alpha),
                new Color(255, 51, 0, alpha), new Color(255, 0, 0, alpha)};
        if (normailizedCount < 1) {
            return new Color(255, 255, 255, 0).getRGB();
        }
        else if (normailizedCount < 30) {
            return colors[0].getRGB();
        }
        else if (normailizedCount < 50) {
            return colors[1].getRGB();
        }
        else if (normailizedCount < 70) {
            return colors[2].getRGB();
        }
        else if (normailizedCount < 100) {
            return colors[3].getRGB();
        }
        else if (normailizedCount < 130) {
            return colors[4].getRGB();
        }
        else if (normailizedCount < 160) {
            return colors[5].getRGB();
        }
        else if (normailizedCount < 190) {
            return colors[6].getRGB();
        }
        else {
            return colors[7].getRGB();
        }
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
        this.CustomizeColor(255, 255, 0, 255, Color.GREEN, true);
        this.Rasterize(sparkContext, spatialRDD, true);
        this.ApplyPhotoFilter(sparkContext);
        this.Colorize();
        this.RenderImage(sparkContext);
        logger.info("[GeoSparkViz][Visualize][Stop]");
        return true;
    }
}
