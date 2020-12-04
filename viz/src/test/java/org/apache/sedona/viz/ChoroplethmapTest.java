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

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.RectangleRDD;
import org.apache.sedona.viz.core.ImageGenerator;
import org.apache.sedona.viz.core.RasterOverlayOperator;
import org.apache.sedona.viz.extension.visualizationEffect.ChoroplethMap;
import org.apache.sedona.viz.extension.visualizationEffect.ScatterPlot;
import org.apache.sedona.viz.utils.ImageType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;

import java.awt.Color;

// TODO: Auto-generated Javadoc

/**
 * The Class ChoroplethmapTest.
 */
public class ChoroplethmapTest
        extends VizTestBase
{
    /**
     * Test rectangle RDD visualization.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRectangleRDDVisualization()
            throws Exception
    {
        PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions, StorageLevel.MEMORY_ONLY());
        RectangleRDD queryRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.KDBTREE);
        queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
        spatialRDD.buildIndex(IndexType.RTREE, true);
        JavaPairRDD<Polygon, Long> joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, queryRDD, true, true);

        ChoroplethMap visualizationOperator = new ChoroplethMap(1000, 600, USMainLandBoundary, false);
        visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true);
        visualizationOperator.Visualize(sparkContext, joinResult);

        ScatterPlot frontImage = new ScatterPlot(1000, 600, USMainLandBoundary, false);
        frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true);
        frontImage.Visualize(sparkContext, queryRDD);

        RasterOverlayOperator overlayOperator = new RasterOverlayOperator(visualizationOperator.rasterImage);
        overlayOperator.JoinImage(frontImage.rasterImage);

        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, "./target/choroplethmap/RectangleRDD-combined", ImageType.PNG);
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
        PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions, StorageLevel.MEMORY_ONLY());
        PolygonRDD queryRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.KDBTREE);
        queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
        spatialRDD.buildIndex(IndexType.RTREE, true);
        JavaPairRDD<Polygon, Long> joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, queryRDD, true, true);

        ChoroplethMap visualizationOperator = new ChoroplethMap(1000, 600, USMainLandBoundary, false);
        visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true);
        visualizationOperator.Visualize(sparkContext, joinResult);

        ScatterPlot frontImage = new ScatterPlot(1000, 600, USMainLandBoundary, false);
        frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true);
        frontImage.Visualize(sparkContext, queryRDD);

        RasterOverlayOperator rasterOverlayOperator = new RasterOverlayOperator(visualizationOperator.rasterImage);
        rasterOverlayOperator.JoinImage(frontImage.rasterImage);

        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(rasterOverlayOperator.backRasterImage, "./target/choroplethmap/PolygonRDD-combined", ImageType.GIF);

        /*
        visualizationOperator = new ChoroplethMap(1000, 600, USMainLandBoundary, false, true);
        visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true);
        visualizationOperator.Visualize(sparkContext, joinResult);

        imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, "./target/choroplethmap/PolygonRDD-combined-1", ImageType.SVG);

        frontImage = new ScatterPlot(1000, 600, USMainLandBoundary, false, true);
        frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true);
        frontImage.Visualize(sparkContext, queryRDD);

        imageGenerator.SaveVectorImageAsLocalFile(frontImage.vectorImage, "./target/choroplethmap/PolygonRDD-combined-2", ImageType.SVG);

        VectorOverlayOperator vectorOverlayOperator = new VectorOverlayOperator(visualizationOperator.vectorImage);
        vectorOverlayOperator.JoinImage(frontImage.vectorImage);

        imageGenerator = new ImageGenerator();
        imageGenerator.SaveVectorImageAsLocalFile(vectorOverlayOperator.backVectorImage, "./target/choroplethmap/PolygonRDD-combined", ImageType.SVG);
        */
    }
}
