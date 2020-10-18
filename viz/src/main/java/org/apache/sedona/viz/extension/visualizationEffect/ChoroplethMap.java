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
import org.apache.sedona.viz.core.VisualizationOperator;
import org.apache.sedona.viz.utils.ColorizeOption;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;

import java.awt.Color;

// TODO: Auto-generated Javadoc

/**
 * The Class ChoroplethMap.
 */
public class ChoroplethMap
        extends VisualizationOperator
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(ChoroplethMap.class);

    /**
     * Instantiates a new choropleth map.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     */
    public ChoroplethMap(int resolutionX, int resolutionY, Envelope datasetBoundary,
            boolean reverseSpatialCoordinate)
    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.NORMAL, reverseSpatialCoordinate, -1, -1, false, false, false);
    }

    /**
     * Instantiates a new choropleth map.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param generateVectorImage the generate vector image
     */
    public ChoroplethMap(int resolutionX, int resolutionY, Envelope datasetBoundary,
            boolean reverseSpatialCoordinate, boolean generateVectorImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.NORMAL, reverseSpatialCoordinate, -1, -1, false, false, generateVectorImage);
    }

    /**
     * Instantiates a new choropleth map.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @param parallelRenderImage the parallel render image
     */
    public ChoroplethMap(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate,
            int partitionX, int partitionY, boolean parallelRenderImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.NORMAL, reverseSpatialCoordinate,
                partitionX, partitionY, false, parallelRenderImage, false);
    }

    /**
     * Instantiates a new choropleth map.
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
    public ChoroplethMap(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate,
            int partitionX, int partitionY, boolean parallelRenderImage, boolean generateVectorImage)
    {
        super(resolutionX, resolutionY, datasetBoundary, ColorizeOption.NORMAL, reverseSpatialCoordinate,
                partitionX, partitionY, false, parallelRenderImage, generateVectorImage);
    }
	
	/*
	@Override
	protected JavaPairRDD<Integer, Color> GenerateColorMatrix()
	{
		//This Color Matrix version controls some too high pixel weights by dividing the max weight to 1/4. 
		logger.debug("[VisualizationOperator][GenerateColorMatrix][Start]");
		final long maxWeight = this.distributedCountMatrix.max(new PixelCountComparator())._2;
		final long minWeight = 0;
		System.out.println("Max weight "+maxWeight);
		JavaPairRDD<Integer, Long> normalizedPixelWeights = this.distributedCountMatrix.mapToPair(new PairFunction<Tuple2<Integer,Long>, Integer, Long>(){
			@Override
			public Tuple2<Integer, Long> call(Tuple2<Integer, Long> pixelWeight) throws Exception {
				if(pixelWeight._2>maxWeight/20)
				{
					return new Tuple2<Integer, Long>(pixelWeight._1,new Long(255));
				}
				return new Tuple2<Integer, Long>(pixelWeight._1,(pixelWeight._2-minWeight)*255/(maxWeight/20-minWeight));
			}});
		this.distributedColorMatrix = normalizedPixelWeights.mapToPair(new PairFunction<Tuple2<Integer,Long>,Integer,Color>()
		{

			@Override
			public Tuple2<Integer, Color> call(Tuple2<Integer, Long> pixelCount) throws Exception {
				Color pixelColor = EncodeColor(pixelCount._2.intValue());
				return new Tuple2<Integer,Color>(pixelCount._1,pixelColor);
			}
		});
		logger.debug("[VisualizationOperator][GenerateColorMatrix][Stop]");
		return this.distributedColorMatrix;
	}
	*/

    /* (non-Javadoc)
     * @see VisualizationOperator#EncodeToColor(int)
     */
    @Override
    protected Color EncodeToColor(int normailizedCount)
            throws Exception
    {
        if (controlColorChannel.equals(Color.RED)) {
            red = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else if (controlColorChannel.equals(Color.GREEN)) {
            green = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else if (controlColorChannel.equals(Color.BLUE)) {
            blue = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else { throw new Exception("[VisualizationOperator][GenerateColor] Unsupported changing color color type. It should be in R,G,B"); }

        if (normailizedCount == 0) {
            return new Color(red, green, blue, 255);
        }
        return new Color(red, green, blue, colorAlpha);
    }

    /**
     * Visualize.
     *
     * @param sparkContext the spark context
     * @param spatialPairRDD the spatial pair RDD
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean Visualize(JavaSparkContext sparkContext, JavaPairRDD<Polygon, Long> spatialPairRDD)
            throws Exception
    {
        logger.info("[Sedona-VizViz][Visualize][Start]");
        this.Rasterize(sparkContext, spatialPairRDD, true);
        this.Colorize();
        this.RenderImage(sparkContext);
        logger.info("[Sedona-VizViz][Visualize][Stop]");
        return true;
    }
}
