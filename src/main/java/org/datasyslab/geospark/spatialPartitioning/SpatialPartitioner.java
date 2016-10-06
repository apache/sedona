package org.datasyslab.geospark.spatialPartitioning;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;

import org.apache.spark.Partitioner;

/**
 * Spatial partitioner is used to partition the data according their spatial partition id.
 *
 */
public class SpatialPartitioner extends Partitioner implements Serializable{

	private int numParts;

	public SpatialPartitioner(int grids)
	{

		numParts=grids;
	}
	
	@Override
	public int getPartition(Object key) {
		// TODO Auto-generated method stub
		return (int)key%(numParts);
	}
	
	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numParts;
	}

}
