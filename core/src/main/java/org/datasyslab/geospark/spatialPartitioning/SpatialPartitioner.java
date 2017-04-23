/**
 * FILE: SpatialPartitioner.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;

import org.apache.spark.Partitioner;

// TODO: Auto-generated Javadoc
/**
 * The Class SpatialPartitioner.
 */
public class SpatialPartitioner extends Partitioner implements Serializable{

	/** The num parts. */
	private int numParts;

	/**
	 * Instantiates a new spatial partitioner.
	 *
	 * @param grids the grids
	 */
	public SpatialPartitioner(int grids)
	{

		numParts=grids+1;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#getPartition(java.lang.Object)
	 */
	@Override
	public int getPartition(Object key) {
		// TODO Auto-generated method stub
		//return (int)key%(numParts);
		return (int)key;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#numPartitions()
	 */
	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numParts;
	}

}
