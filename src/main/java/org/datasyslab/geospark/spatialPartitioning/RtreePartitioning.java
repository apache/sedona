package org.datasyslab.geospark.spatialPartitioning;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

/**
 * 
 * Construct a grid file. Each grid is a rtree leaf node.
 *
 */
public class RtreePartitioning implements Serializable{

	HashSet<EnvelopeWithGrid> grids=new HashSet<EnvelopeWithGrid>();
	
	/**
	 * 
	 * @param SampleList Sample taken from the entire dataset
	 * @param boundary Boundary of the entire dataset
	 * @param partitions Grids of the grid file
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
	 * 
	 * @param SampleList Sample taken from the entire dataset
	 * @param boundary Boundary of the entire dataset
	 * @param partitions Grids of the grid file
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
	 * 
	 * @param SampleList Sample taken from the entire dataset
	 * @param boundary Boundary of the entire dataset
	 * @param partitions Grids of the grid file
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
	 * @return Return the generated grid file
	 */
	public HashSet<EnvelopeWithGrid> getGrids() {
		
		return this.grids;
		
	}
}
