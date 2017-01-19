/**
 * FILE: PartitionJudgement.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.PartitionJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;


import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PartitionJudgement.
 */
public class PartitionJudgement implements Serializable{
	
	
	/**
	 * Gets the partition ID.
	 *
	 * @param grids the grids
	 * @param spatialObject the spatial object
	 * @return the partition ID
	 * @throws Exception the exception
	 */
	public static Iterator<Tuple2<Integer, Object>> getPartitionID(List<Envelope> grids,Object spatialObject) throws Exception
	{
		HashSet<Tuple2<Integer, Object>> result = new HashSet<Tuple2<Integer, Object>>();
		int overflowContainerID=grids.size();
		boolean containFlag=false;
		for(int gridId=0;gridId<grids.size();gridId++)
		{
			if(spatialObject instanceof Envelope)
			{
				Envelope castedSpatialObject = (Envelope) spatialObject;
				if(grids.get(gridId).contains(castedSpatialObject))
				{
					result.add(new Tuple2<Integer, Object>(gridId, castedSpatialObject));
					containFlag=true;
				}
				else if (grids.get(gridId).intersects(castedSpatialObject)||castedSpatialObject.contains(grids.get(gridId)))
				{		
					result.add(new Tuple2<Integer, Object>(gridId, castedSpatialObject));
					//containFlag=true;
				}
			}
			else if(spatialObject instanceof Geometry)
			{
				Geometry castedSpatialObject = (Geometry) spatialObject;
				if(grids.get(gridId).contains(castedSpatialObject.getEnvelopeInternal()))
				{
					result.add(new Tuple2<Integer, Object>(gridId, castedSpatialObject));
					containFlag=true;
				}
				else if(grids.get(gridId).intersects(castedSpatialObject.getEnvelopeInternal())||castedSpatialObject.getEnvelopeInternal().contains(grids.get(gridId)))
				{
					result.add(new Tuple2<Integer, Object>(gridId, castedSpatialObject));
					//containFlag=true;
				}
			}
			else
			{
				throw new Exception("[PartitionJudgement][getPartitionID] Unsupported spatial object type");
			}

		}
		if(containFlag==false)
		{
			result.add(new Tuple2<Integer, Object>(overflowContainerID,spatialObject));
		}
		return result.iterator();
	}
}
