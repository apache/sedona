/**
 * FILE: RtreePartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.RtreePartitioning.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import com.vividsolutions.jts.index.strtree.STRtree;

// TODO: Auto-generated Javadoc
/**
 * The Class RtreePartitioning.
 */
public class RtreePartitioning implements Serializable{

	/** The grids. */
	List<Envelope> grids=new ArrayList<Envelope>();
	
	
	/**
	 * Instantiates a new rtree partitioning.
	 *
	 * @param SampleList the sample list
	 * @param boundary the boundary
	 * @param partitions the partitions
	 * @throws Exception the exception
	 */
	public RtreePartitioning(List SampleList,Envelope boundary,int partitions) throws Exception
	{
		STRtree strtree=new STRtree(SampleList.size()/partitions);
    	for(int i=0;i<SampleList.size();i++)
    	{
    		if(SampleList.get(i) instanceof Envelope)
    		{
    			Envelope spatialObject = (Envelope)SampleList.get(i);
    			strtree.insert(spatialObject, spatialObject);
    		}
    		else if(SampleList.get(i) instanceof Geometry)
    		{
    			Geometry spatialObject = (Geometry)SampleList.get(i);
    			strtree.insert(spatialObject.getEnvelopeInternal(), spatialObject);
    		}
    		else
    		{
    			throw new Exception("[RtreePartitioning][Constrcutor] Unsupported spatial object type");
    		}
    	}
    	List<Envelope> envelopes=strtree.queryBoundary();
    	for(int i=0;i<envelopes.size();i++)
    	{
    		grids.add(envelopes.get(i));
    	}
    	//grids.add(new EnvelopeWithGrid(boundary,grids.size()));
	}
	
	/**
	 * Gets the grids.
	 *
	 * @return the grids
	 */
	public List<Envelope> getGrids() {
		
		return this.grids;
		
	}
}
