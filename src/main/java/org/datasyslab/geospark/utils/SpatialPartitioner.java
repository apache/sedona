package org.datasyslab.geospark.utils;

import java.io.Serializable;

import org.apache.spark.Partitioner;

/**
 * Spatial partitioner is used to partition the data according their spatial partition id.
 *
 */
public class SpatialPartitioner extends Partitioner implements Serializable{

	private int numParts;

	public SpatialPartitioner(int numParts)
	{
	  this.numParts = numParts;
	}
	@Override
	public int getPartition(Object key) {
		// TODO Auto-generated method stub
		return (int)key%(numParts-1);
	}
	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numParts;
	}

}
