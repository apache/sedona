/**
 * FILE: RtreePartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.RtreePartitioning.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

// TODO: Auto-generated Javadoc
/**
 * The Class RtreePartitioning.
 */
public class RtreePartitioning implements Serializable{

	/** The grids. */
	HashSet<EnvelopeWithGrid> grids=new HashSet<EnvelopeWithGrid>();
	
	/**
	 * Instantiates a new rtree partitioning.
	 *
	 * @param SampleList the sample list
	 * @param boundary the boundary
	 * @param partitions the partitions
	 */
	public RtreePartitioning(Point[] SampleList,Envelope boundary,int partitions)
	{
		STRtree strtree=new STRtree(SampleList.length/partitions);
    	for(int i=0;i<SampleList.length;i++)
    	{
    		strtree.insert(SampleList[i].getEnvelopeInternal(), SampleList[i]);
    	}
    	List<Envelope> envelopes=strtree.queryBoundary();
    	for(int i=0;i<envelopes.size();i++)
    	{
    		grids.add(new EnvelopeWithGrid(envelopes.get(i),i));
    	}
    	//grids.add(new EnvelopeWithGrid(boundary,grids.size()));
	}
	
	/**
	 * Instantiates a new rtree partitioning.
	 *
	 * @param SampleList the sample list
	 * @param boundary the boundary
	 * @param partitions the partitions
	 */
	public RtreePartitioning(Envelope[] SampleList,Envelope boundary,int partitions)
	{
		STRtree strtree=new STRtree(SampleList.length/partitions);
    	for(int i=0;i<SampleList.length;i++)
    	{
    		strtree.insert(SampleList[i], SampleList[i]);
    	}
    	List<Envelope> envelopes=strtree.queryBoundary();
    	for(int i=0;i<envelopes.size();i++)
    	{
    		grids.add(new EnvelopeWithGrid(envelopes.get(i),i));
    	}
    	//grids.add(new EnvelopeWithGrid(boundary,grids.size()));
	}
	
	/**
	 * Instantiates a new rtree partitioning.
	 *
	 * @param SampleList the sample list
	 * @param boundary the boundary
	 * @param partitions the partitions
	 */
	public RtreePartitioning(Polygon[] SampleList,Envelope boundary,int partitions)
	{
		STRtree strtree=new STRtree(SampleList.length/partitions);
    	for(int i=0;i<SampleList.length;i++)
    	{
    		strtree.insert(SampleList[i].getEnvelopeInternal(), SampleList[i]);
    	}
    	List<Envelope> envelopes=strtree.queryBoundary();
    	for(int i=0;i<envelopes.size();i++)
    	{
    		grids.add(new EnvelopeWithGrid(envelopes.get(i),i));
    	}
    	//grids.add(new EnvelopeWithGrid(boundary,grids.size()));
    	
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
