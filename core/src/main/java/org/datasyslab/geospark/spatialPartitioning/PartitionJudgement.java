/**
 * FILE: PartitionJudgement.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.PartitionJudgement.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
	public static <T extends Geometry> Iterator<Tuple2<Integer, T>> getPartitionID(List<Envelope> grids, T spatialObject) throws Exception
	{
		Set<Tuple2<Integer, T>> result = new HashSet();
		int overflowContainerID=grids.size();
		boolean containFlag=false;
		for(int gridId=0;gridId<grids.size();gridId++)
		{
			final Envelope envelope = spatialObject.getEnvelopeInternal();
			final Envelope grid = grids.get(gridId);
			if(grid.covers(envelope))
			{
				result.add(new Tuple2(gridId, spatialObject));
				containFlag=true;
			}
			else if(grid.intersects(envelope) || envelope.covers(grid))
			{
				result.add(new Tuple2(gridId, spatialObject));
				//containFlag=true;
			}
		}
		if(containFlag==false)
		{
			result.add(new Tuple2(overflowContainerID, spatialObject));
		}
		return result.iterator();
	}

	public static <T extends Geometry> Iterator<Tuple2<Integer, T>> getPartitionID(StandardQuadTree partitionTree, T spatialObject) throws Exception {
		Set<Tuple2<Integer, T>> result = new HashSet<>();
		boolean containFlag = false;
		ArrayList<QuadRectangle> matchedPartitions = new ArrayList<QuadRectangle>();
		try {
			partitionTree.getZone(matchedPartitions, new QuadRectangle((spatialObject).getEnvelopeInternal()));
		}
		catch (NullPointerException e)
		{
			return result.iterator();
		}
		for(int i=0;i<matchedPartitions.size();i++)
		{
			containFlag=true;
			result.add(new Tuple2(matchedPartitions.get(i).partitionId, spatialObject));
		}

		if (containFlag == false) {
			//throw new Exception("This object cannot find partition: " +spatialObject);

			// This object is not covered by the partition. Should be dropped.
			// Partition tree from StandardQuadTree do not have missed objects.
		}
		return result.iterator();
	}
}
