/**
 * FILE: ChoroplethMap.java
 * PATH: org.datasyslab.babylon.extension.visualizationEffect.ChoroplethMap.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.visualizationEffect;

import java.awt.Color;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.core.VisualizationOperator;

import com.vividsolutions.jts.geom.Envelope;

/**
 * The Class ChoroplethMap.
 */
public class ChoroplethMap extends VisualizationOperator{

	/**
	 * Instantiates a new choropleth map.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 */
	public ChoroplethMap(int resolutionX, int resolutionY, Envelope datasetBoundary,
			boolean reverseSpatialCoordinate) {
		super(resolutionX, resolutionY, datasetBoundary, false, reverseSpatialCoordinate);
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
	public ChoroplethMap(int resolutionX, int resolutionY, Envelope datasetBoundary,boolean reverseSpatialCoordinate,
			int partitionX, int partitionY, boolean parallelRenderImage) {
		super(resolutionX, resolutionY, datasetBoundary, false, reverseSpatialCoordinate,
				partitionX, partitionY, false, parallelRenderImage);
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
	 * @see org.datasyslab.babylon.core.VisualizationOperator#EncodeToColor(int)
	 */
	@Override
	protected Color EncodeToColor(int normailizedCount) throws Exception
	{
		if(controlColorChannel.equals(Color.RED))
		{
			red=useInverseRatioForControlColorChannel?255-normailizedCount:normailizedCount;
		}
		else if(controlColorChannel.equals(Color.GREEN))
		{
			green=useInverseRatioForControlColorChannel?255-normailizedCount:normailizedCount;
		}
		else if(controlColorChannel.equals(Color.BLUE))
		{
			blue=useInverseRatioForControlColorChannel?255-normailizedCount:normailizedCount;
		}
		else throw new Exception("[VisualizationOperator][GenerateColor] Unsupported changing color color type. It should be in R,G,B");
		
		if(normailizedCount==0)
		{
			return new Color(red,green,blue,255);
		}
		return new Color(red,green,blue,colorAlpha);
	}
	
	/**
	 * Visualize.
	 *
	 * @param sparkContext the spark context
	 * @param spatialPairRDD the spatial pair RDD
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean Visualize(JavaSparkContext sparkContext, JavaPairRDD spatialPairRDD) throws Exception {		
		this.Rasterize(sparkContext, spatialPairRDD, true);
		this.GenerateColorMatrix();
		this.RenderImage();
		return true;
	}

}
