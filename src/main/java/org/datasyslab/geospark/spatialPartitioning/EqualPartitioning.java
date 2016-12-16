/**
 * FILE: EqualPartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.EqualPartitioning.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;

import com.vividsolutions.jts.geom.Envelope;

// TODO: Auto-generated Javadoc
/**
 * The Class EqualPartitioning.
 */
public class EqualPartitioning implements Serializable{

	/** The grids. */
	HashSet<EnvelopeWithGrid> grids=new HashSet<EnvelopeWithGrid>();

	
	/**
	 * Instantiates a new equal partitioning.
	 *
	 * @param boundary the boundary
	 * @param partitions the partitions
	 */
	public  EqualPartitioning(Envelope boundary,int partitions)
	{
		//Local variable should be declared here
		Double root=Math.sqrt(partitions);
		int partitionsAxis;
		double intervalX;
		double intervalY;
		List<Double> boundsX=new ArrayList<Double>();
		List<Double> boundsY=new ArrayList<Double>();
		//Calculate how many bounds should be on each axis
		partitionsAxis=root.intValue();
		intervalX=(boundary.getMaxX()-boundary.getMinX())/partitionsAxis;
		intervalY=(boundary.getMaxY()-boundary.getMinY())/partitionsAxis;
		for(int i=0;i<partitionsAxis;i++)
		{
			boundsX.add(boundary.getMinX()+i*intervalX);
			boundsY.add(boundary.getMinY()+i*intervalY);
		}
		boundsX.add(boundary.getMaxX());
		boundsY.add(boundary.getMaxY());
		for(int i=0;i<boundsX.size()-1;i++)
		{
			for(int j=0;j<boundsY.size()-1;j++)
			{
				Envelope grid=new Envelope(boundsX.get(i),boundsX.get(i+1),boundsY.get(j),boundsY.get(j+1));
				grids.add(new EnvelopeWithGrid(grid,i*boundsY.size()+j));
			}
		}

		
	}


	/**
	 * Gets the grids.
	 *
	 * @return the grids
	 */
	public HashSet<EnvelopeWithGrid> getGrids() {
		
		return this.grids;
		
	}
}
