/**
 * FILE: PartitionJudgement.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.PartitionJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PartitionJudgement.
 */
public class PartitionJudgement implements Serializable{
	
	/**
	 * Instantiates a new partition judgement.
	 */
	public PartitionJudgement()
	{
		
	}
	
	/**
	 * Gets the partition ID.
	 *
	 * @param grid the grid
	 * @param point the point
	 * @return the partition ID
	 */
	public static Iterator<Tuple2<Integer, Point>> getPartitionID(HashSet<EnvelopeWithGrid> grid,Point point)
	{
		HashSet<Tuple2<Integer, Point>> result = new HashSet<Tuple2<Integer, Point>>();
		Iterator<EnvelopeWithGrid> iteratorGrid=grid.iterator();
		EnvelopeWithGrid gridInstance=null;
		int overflowContainerID=grid.size();
		while(iteratorGrid.hasNext())
		{
			gridInstance=iteratorGrid.next();
			if(gridInstance.contains(point.getCoordinate()))
			{
				result.add(new Tuple2<Integer, Point>(gridInstance.grid,point));
				 
			}
		}

		result.add(new Tuple2<Integer, Point>(overflowContainerID,point));
		return result.iterator();
	}
	
	/**
	 * Gets the partition ID.
	 *
	 * @param grid the grid
	 * @param envelope the envelope
	 * @return the partition ID
	 */
	public static HashSet<Tuple2<Integer, Envelope>> getPartitionID(HashSet<EnvelopeWithGrid> grid,Envelope envelope)
	{
		HashSet<Tuple2<Integer, Envelope>> result = new HashSet<Tuple2<Integer, Envelope>>();
		Iterator<EnvelopeWithGrid> iteratorGrid=grid.iterator();
		EnvelopeWithGrid gridInstance=null;
		int overflowContainerID=grid.size();
		boolean containFlag=false;
		while(iteratorGrid.hasNext())
		{
			gridInstance=iteratorGrid.next();
			if(gridInstance.contains(envelope))
			{
				result.add(new Tuple2<Integer, Envelope>(gridInstance.grid, envelope));
				containFlag=true;
			}
			else if (gridInstance.intersects(envelope)||envelope.contains(gridInstance))
			{		
				result.add(new Tuple2<Integer, Envelope>(gridInstance.grid, envelope));
			}
		}
		if(containFlag==false)
		{
			result.add(new Tuple2<Integer, Envelope>(overflowContainerID,envelope));
		}
		return result;
	}
	
	/**
	 * Gets the partition ID.
	 *
	 * @param grid the grid
	 * @param polygon the polygon
	 * @return the partition ID
	 */
	public static HashSet<Tuple2<Integer, Polygon>> getPartitionID(HashSet<EnvelopeWithGrid> grid,Polygon polygon)
	{
		HashSet<Tuple2<Integer, Polygon>> result = new HashSet<Tuple2<Integer, Polygon>>();
		Iterator<EnvelopeWithGrid> iteratorGrid=grid.iterator();
		EnvelopeWithGrid gridInstance=null;
		int overflowContainerID=grid.size();
		boolean containFlag=false;
		while(iteratorGrid.hasNext())
		{
			gridInstance=iteratorGrid.next();
			if(gridInstance.contains(polygon.getEnvelopeInternal()))
			{
				result.add(new Tuple2<Integer, Polygon>(gridInstance.grid, polygon));
				containFlag=true;
			}
			else if(gridInstance.intersects(polygon.getEnvelopeInternal())||polygon.getEnvelopeInternal().contains(gridInstance))
			{
				result.add(new Tuple2<Integer, Polygon>(gridInstance.grid, polygon));	
			}
		}
		if(containFlag==false)
		{
			result.add(new Tuple2<Integer, Polygon>(overflowContainerID,polygon));
		}
		return result;
	}
	
}
