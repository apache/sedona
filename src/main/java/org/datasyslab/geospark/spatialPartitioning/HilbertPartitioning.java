/**
 * FILE: HilbertPartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;


// TODO: Auto-generated Javadoc
/**
 * The Class HilbertPartitioning.
 */
public class HilbertPartitioning implements Serializable{

	/** The splits. */
	//Partition ID
	protected int[] splits;
	
	/** The grids. */
	List<Envelope> grids=new ArrayList<Envelope>();

	/**
	 * Instantiates a new hilbert partitioning.
	 *
	 * @param SampleList the sample list
	 * @param boundary the boundary
	 * @param partitions the partitions
	 * @throws Exception the exception
	 */
	public HilbertPartitioning(List SampleList,Envelope boundary,int partitions) throws Exception
	{
		//this.boundary=boundary;
		int gridResolution=Short.MAX_VALUE;
	    int[] hValues = new int[SampleList.size()];
	    Envelope [] gridWithoutID=new Envelope[partitions];
	    if(SampleList.get(0) instanceof Envelope)
	    {
		    for (int i = 0; i < SampleList.size(); i++){
		    	Envelope spatialObject = (Envelope)SampleList.get(i);
		    	int x=locationMapping(boundary.getMinX(),boundary.getMaxX(),(spatialObject.getMinX()+spatialObject.getMaxX())/2.0);
		    	int y=locationMapping(boundary.getMinY(),boundary.getMaxY(),(spatialObject.getMinY()+spatialObject.getMaxY())/2.0);
		    	hValues[i] = computeHValue(gridResolution+1,x,y);
		    }
	    }
	    else if(SampleList.get(0) instanceof Geometry)
	    {
		    for (int i = 0; i < SampleList.size(); i++){
			      Envelope envelope=((Geometry)SampleList.get(i)).getEnvelopeInternal();
			      int x=locationMapping(boundary.getMinX(),boundary.getMaxX(),(envelope.getMinX()+envelope.getMaxX())/2.0);
			      int y=locationMapping(boundary.getMinY(),boundary.getMaxY(),(envelope.getMinY()+envelope.getMaxY())/2.0);
			      hValues[i] = computeHValue(gridResolution+1,x,y);
			    }
	    }
	    else
	    {
	    	throw new Exception("[HilbertPartitioning][Constrcutor] Unsupported spatial object type");
	    }

	    createFromHValues(hValues, partitions);
	    for(int i=0;i<SampleList.size();i++)
	    {
	    	Envelope initialBoundary=null;
	    	Object spatialObject = SampleList.get(i);
	    	if(SampleList.get(0) instanceof Envelope)
	    	{
	    		initialBoundary = (Envelope)spatialObject;
	    	}
	    	else if(SampleList.get(0) instanceof Geometry)
	    	{
	    		initialBoundary = ((Geometry)spatialObject).getEnvelopeInternal();
	    	}
	    	else
	    	{
	    		throw new Exception("[HilbertPartitioning][Constrcutor] Unsupported spatial object type");
	    	}
	    	int partitionID=gridID(boundary,SampleList.get(i),splits);
	    	gridWithoutID[partitionID]=initialBoundary;
	    }
	    for(int i=0;i<SampleList.size();i++)
	    {
	    	int partitionID=gridID(boundary,SampleList.get(i),splits);
	    	gridWithoutID[partitionID]=updateEnvelope(gridWithoutID[partitionID],SampleList.get(i));
	    }
	    for(int i=0;i<gridWithoutID.length;i++)
	    {
	    	this.grids.add(gridWithoutID[i]);
	    }
	}
	
	  /**
  	 * Creates the from H values.
  	 *
  	 * @param hValues the h values
  	 * @param partitions the partitions
  	 */
	  protected void createFromHValues(int[] hValues, int partitions) {
	    Arrays.sort(hValues);

	    this.splits = new int[partitions];
	    int maxH = 0x7fffffff;
	    for (int i = 0; i < splits.length; i++) {
	      int quantile = (int) ((long)(i + 1) * hValues.length / partitions);
	      this.splits[i] = quantile == hValues.length ? maxH : hValues[quantile];
	    }
	  }
	
	/**
	 * Compute H value.
	 *
	 * @param n the n
	 * @param x the x
	 * @param y the y
	 * @return the int
	 */
	  public static int computeHValue(int n, int x, int y) {
	    int h = 0;
	    for (int s = n/2; s > 0; s/=2) {
	      int rx = (x & s) > 0 ? 1 : 0;
	      int ry = (y & s) > 0 ? 1 : 0;
	      h += s * s * ((3 * rx) ^ ry);

	      // Rotate
	      if (ry == 0) {
	        if (rx == 1) {
	          x = n-1 - x;
	          y = n-1 - y;
	        }

	        //Swap x and y
	        int t = x; x = y; y = t;
	      }
	    }
	    return h;
	  }
	  
  	/**
	   * Gets the partition bounds.
	   *
	   * @return the partition bounds
	   */
  	public int[] getPartitionBounds()
	  {
		  return splits;
	  }
	  
  	/**
	   * Location mapping.
	   *
	   * @param axisMin the axis min
	   * @param axisLocation the axis location
	   * @param axisMax the axis max
	   * @return the int
	   */
  	public static int locationMapping (double axisMin, double axisLocation,double axisMax)
	  {
		  Double gridLocation;
		  int gridResolution=Short.MAX_VALUE;
		  gridLocation=(axisLocation-axisMin)*gridResolution/(axisMax-axisMin);
		  return gridLocation.intValue();
	  }
	  
  	
  	/**
	   * Grid ID.
	   *
	   * @param boundary the boundary
	   * @param spatialObject the spatial object
	   * @param partitionBounds the partition bounds
	   * @return the int
	   * @throws Exception the exception
	   */
	  public static int gridID(Envelope boundary,Object spatialObject,int[] partitionBounds) throws Exception
  	{
  		int x = 0;
  		int y = 0;
  		if(spatialObject instanceof Envelope)
  		{
  			x=locationMapping(boundary.getMinX(),boundary.getMaxX(),(((Envelope)spatialObject).getMinX()+((Envelope)spatialObject).getMaxX())/2.0);
  			y=locationMapping(boundary.getMinY(),boundary.getMaxY(),(((Envelope)spatialObject).getMinY()+((Envelope)spatialObject).getMaxY())/2.0);
  		}
  		else if(spatialObject instanceof Geometry)
  		{
  			Envelope envelope=((Geometry)spatialObject).getEnvelopeInternal();
  			x=locationMapping(boundary.getMinX(),boundary.getMaxX(),(envelope.getMinX()+envelope.getMaxX())/2.0);
  			y=locationMapping(boundary.getMinY(),boundary.getMaxY(),(envelope.getMinY()+envelope.getMaxY())/2.0);
  		}
  		else
  		{
			throw new Exception("[HilbertPartitioning][gridID] Unsupported spatial object type");
  		}
		int gridResolution=Short.MAX_VALUE;
		int hValue = computeHValue(gridResolution+1,x,y);
		int partition = Arrays.binarySearch(partitionBounds, hValue);
		if (partition < 0)
		   partition = -partition - 1;
		return partition;
  	}
		
		/**
		 * Update envelope.
		 *
		 * @param envelope the envelope
		 * @param spatialObject the spatial object
		 * @return the envelope
		 * @throws Exception the exception
		 */
		public static Envelope updateEnvelope(Envelope envelope, Object spatialObject) throws Exception
		{
			double minX=envelope.getMinX();
			double maxX=envelope.getMaxX();
			double minY=envelope.getMinY();
			double maxY=envelope.getMaxY();
			if(spatialObject instanceof Envelope)
			{
				Envelope i = (Envelope)spatialObject;
				if(minX>i.getMinX())
				{
					minX=i.getMinX();
				}
				if(maxX<i.getMaxX())
				{
					maxX=i.getMaxX();
				}
				if(minY>i.getMinY())
				{
					minY=i.getMinY();
				}
				if(maxY<i.getMaxY())
				{
					maxY=i.getMaxY();
				}
			}
			else if(spatialObject instanceof Geometry)
			{
				Envelope i=((Geometry)spatialObject).getEnvelopeInternal();
				if(minX>i.getMinX())
				{
					minX=i.getMinX();
				}
				if(maxX<i.getMaxX())
				{
					maxX=i.getMaxX();
				}
				if(minY>i.getMinY())
				{
					minY=i.getMinY();
				}
				if(maxY<i.getMaxY())
				{
					maxY=i.getMaxY();
				}
			}
			else
			{
				throw new Exception("[HilbertPartitioning][updateEnvelope] Unsupported spatial object type");
			}
			return new Envelope(minX,maxX,minY,maxY);
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
