/**
 * FILE: RtreePartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.RtreePartitioning.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class RtreePartitioning.
 */
public class RtreePartitioning implements Serializable{

	/** The grids. */
	final List<Envelope> grids = new ArrayList<>();
	
	
	/**
	 * Instantiates a new rtree partitioning.
	 *
	 * @param samples the sample list
	 * @param partitions the partitions
	 * @throws Exception the exception
	 */
	public RtreePartitioning(List<Envelope> samples, int partitions) throws Exception
	{
		STRtree strtree=new STRtree(samples.size()/partitions);
		for (Envelope sample : samples) {
			strtree.insert(sample, sample);
    	}

    	List<Envelope> envelopes=strtree.queryBoundary();
		for (Envelope envelope : envelopes) {
    		grids.add(envelope);
    	}
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
