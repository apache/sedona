/**
 * FILE: ScatterPlot.java
 * PATH: org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.visualizationEffect;

import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.core.VisualizationOperator;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import com.vividsolutions.jts.geom.Envelope;

/**
 * The Class ScatterPlot.
 */
public class ScatterPlot extends VisualizationOperator {
	
	/**
	 * Instantiates a new scatter plot.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 */
	public ScatterPlot(int resolutionX, int resolutionY, Envelope datasetBoundary, boolean reverseSpatialCoordinate) {
		super(resolutionX, resolutionY, datasetBoundary,false,reverseSpatialCoordinate);
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
	public ScatterPlot(int resolutionX, int resolutionY,Envelope datasetBoundary, boolean reverseSpatialCoordinate,
			int partitionX, int partitionY, boolean parallelRenderImage) {
		super(resolutionX, resolutionY, datasetBoundary,false,reverseSpatialCoordinate,
			partitionX, partitionY, false, parallelRenderImage);
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
		this.Rasterize(sparkContext, spatialRDD, true);
		this.GenerateColorMatrix();
		this.RenderImage();
		return true;
	}


}
