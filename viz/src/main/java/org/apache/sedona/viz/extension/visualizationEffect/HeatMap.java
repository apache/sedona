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
package org.apache.sedona.viz.extension.visualizationEffect;

import org.apache.log4j.Logger;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.viz.core.VisualizationOperator;
import org.apache.sedona.viz.extension.photoFilter.GaussianBlur;
import org.apache.sedona.viz.utils.ColorizeOption;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Envelope;

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
     * @see VisualizationOperator#EncodeToColor(int)
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
        logger.info("[Sedona-Viz][Visualize][Start]");
        this.CustomizeColor(255, 255, 0, 255, Color.GREEN, true);
        this.Rasterize(sparkContext, spatialRDD, true);
        this.ApplyPhotoFilter(sparkContext);
        this.Colorize();
        this.RenderImage(sparkContext);
        logger.info("[Sedona-Viz][Visualize][Stop]");
        return true;
    }
}
