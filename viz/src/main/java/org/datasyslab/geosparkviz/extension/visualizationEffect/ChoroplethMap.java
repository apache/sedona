/*
 * FILE: ChoroplethMap
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
import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geosparkviz.core.VisualizationOperator;
import org.datasyslab.geosparkviz.utils.ColorizeOption;

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
     * @see org.datasyslab.geosparkviz.core.VisualizationOperator#EncodeToColor(int)
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
        logger.info("[GeoSparkViz][Visualize][Start]");
        this.Rasterize(sparkContext, spatialPairRDD, true);
        this.Colorize();
        this.RenderImage(sparkContext);
        logger.info("[GeoSparkViz][Visualize][Stop]");
        return true;
    }
}
