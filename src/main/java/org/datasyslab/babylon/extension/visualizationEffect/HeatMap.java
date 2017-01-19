/**
 * FILE: HeatMap.java
 * PATH: org.datasyslab.babylon.extension.visualizationEffect.HeatMap.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.visualizationEffect;

import java.awt.Color;


import org.apache.spark.api.java.JavaSparkContext;

import org.datasyslab.babylon.core.VisualizationOperator;
import org.datasyslab.babylon.extension.photoFilter.GaussianBlur;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import com.vividsolutions.jts.geom.Envelope;



/**
 * The Class HeatMap.
 */
public class HeatMap extends VisualizationOperator{

	/**
	 * Instantiates a new heat map.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @param blurRadius the blur radius
	 */
	public HeatMap(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate,int blurRadius)
	
	{
		super(resolutionX, resolutionY, datasetBoundary, true, reverseSpatialCoordinate);
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
	public HeatMap(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate,int blurRadius,
			int partitionX, int partitionY, boolean parallelPhotoFilter, boolean parallelRenderImage)
	
	{
		super(resolutionX, resolutionY, datasetBoundary, true, reverseSpatialCoordinate,
				partitionX, partitionY, parallelPhotoFilter, parallelRenderImage);
		GaussianBlur gaussianBlur = new GaussianBlur(blurRadius);
		this.InitPhotoFilterWeightMatrix(gaussianBlur);
	}

	
	/* (non-Javadoc)
	 * @see org.datasyslab.babylon.core.VisualizationOperator#EncodeToColor(int)
	 */
	@Override
	protected Color EncodeToColor(int normailizedCount) throws Exception
	{
		int alpha = 150;
		Color[] colors = new Color[]{new Color(153,255,0,alpha),new Color(204,255,0,alpha),new Color(255,255,0,alpha),
		      new Color(255,204,0,alpha),new Color(255,153,0,alpha),new Color(255,102,0,alpha),
		      new Color(255,51,0,alpha),new Color(255,0,0,alpha)};
	    if (normailizedCount < 1){
	        return new Color(255,255,255,0);
	    }
	    else if(normailizedCount<30)
	    {
	    	return colors[0];
	    }
	    else if(normailizedCount<50)
	    {
	    	return colors[1];
	    }
	    else if(normailizedCount<70)
	    {
	    	return colors[2];
	    }
	    else if(normailizedCount<100)
	    {
	    	return colors[3];
	    }
	    else if(normailizedCount<130)
	    {
	    	return colors[4];
	    }
	    else if(normailizedCount<160)
	    {
	    	return colors[5];
	    }
	    else if(normailizedCount<190)
	    {
	    	return colors[6];
	    }
	    else
	    {
	    	return colors[7];
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
	public boolean Visualize(JavaSparkContext sparkContext, SpatialRDD spatialRDD) throws Exception {
		this.CustomizeColor(255, 255, 0, 255, Color.GREEN, true);
		this.Rasterize(sparkContext, spatialRDD, true);
		this.ApplyPhotoFilter(sparkContext);
		this.GenerateColorMatrix();
		this.RenderImage();
		return true;
	}

}
