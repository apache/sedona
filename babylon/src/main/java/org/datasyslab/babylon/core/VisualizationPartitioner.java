/**
 * FILE: VisualizationPartitioner.java
 * PATH: org.datasyslab.babylon.core.VisualizationPartitioner.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import java.io.Serializable;

import org.apache.spark.Partitioner;
import org.datasyslab.babylon.utils.RasterizationUtils;

import scala.Tuple2;

/**
 * The Class VisualizationPartitioner.
 */
public class VisualizationPartitioner extends Partitioner implements Serializable{

	/** The partition interval Y. */
	public int resolutionX,resolutionY,partitionX,partitionY,partitionIntervalX,partitionIntervalY;
	
	
	/**
	 * Instantiates a new visualization partitioner.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param partitionX the partition X
	 * @param partitionY the partition Y
	 * @throws Exception the exception
	 */
	public VisualizationPartitioner(int resolutionX, int resolutionY, int partitionX, int partitionY) throws Exception
	{
		this.resolutionX = resolutionX;
		this.resolutionY = resolutionY;
		this.partitionX = partitionX;
		this.partitionY = partitionY;
		if(this.resolutionX%partitionX!=0||this.resolutionY%partitionY!=0)
		{
			throw new Exception("[VisualizationPartitioner][Constructor] The given partition number fails to exactly divide the corresponding resolution axis.");
		}
		this.partitionIntervalX = this.resolutionX / this.partitionX;
		this.partitionIntervalY = this.resolutionY / this.partitionY;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#getPartition(java.lang.Object)
	 */
	@Override
	public int getPartition(Object key) {
		//This key is the key in <Key, Value> PairRDD
		Tuple2<Integer,Integer> pixelCoordinate = RasterizationUtils.Decode1DTo2DId(this.resolutionX, this.resolutionY, (int)key);
		//This two find the parition coordinate of a given pixel. For example, Pixel (1,1) should be at Image Partition (1,1).
		int partitionCoordinateX = pixelCoordinate._1/this.partitionIntervalX;
		int partitionCoordinateY = pixelCoordinate._2/this.partitionIntervalY;
		try {
			int partitionId = RasterizationUtils.Encode2DTo1DId(this.partitionX, this.partitionY, partitionCoordinateX, partitionCoordinateY);
			return partitionId;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// We should not reach here. This means the partition id is wrong.
		return -1;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#numPartitions()
	 */
	@Override
	public int numPartitions() {
		return partitionX*partitionY;
	}

}
