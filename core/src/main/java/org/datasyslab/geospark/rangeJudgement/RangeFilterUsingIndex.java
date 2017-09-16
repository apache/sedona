/**
 * FILE: RangeFilterUsingIndex.java
 * PATH: org.datasyslab.geospark.rangeJudgement.RangeFilterUsingIndex.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

// TODO: Auto-generated Javadoc
/**
 * The Class RangeFilterUsingIndex.
 */
public class RangeFilterUsingIndex implements FlatMapFunction<Iterator<Object>,Object>, Serializable{

	/** The consider boundary intersection. */
	boolean considerBoundaryIntersection=false;
	
	/** The query window. */
	Object queryWindow;

	/**
	 * Instantiates a new range filter using index.
	 *
	 * @param queryWindow the query window
	 * @param considerBoundaryIntersection the consider boundary intersection
	 */
	public RangeFilterUsingIndex(Envelope queryWindow,boolean considerBoundaryIntersection)
	{
		this.considerBoundaryIntersection=considerBoundaryIntersection;
		this.queryWindow=queryWindow;

	}
	
	/**
	 * Instantiates a new range filter using index.
	 *
	 * @param queryWindow the query window
	 * @param considerBoundaryIntersection the consider boundary intersection
	 */
	public RangeFilterUsingIndex(Polygon queryWindow,boolean considerBoundaryIntersection)
	{
		this.considerBoundaryIntersection=considerBoundaryIntersection;
		this.queryWindow=queryWindow;

	}
	
	/**
	 * Call.
	 *
	 * @param treeIndexes the tree indexes
	 * @return the iterator
	 * @throws Exception the exception
	 */
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public List<Object> call(Iterator<Object> treeIndexes) throws Exception {
		assert treeIndexes.hasNext()==true;
		Object treeIndex = treeIndexes.next();
		if(treeIndex instanceof STRtree)
		{
			STRtree strtree= (STRtree) treeIndex;
			List<Object> result=new ArrayList<Object>();
			if(this.queryWindow instanceof Envelope)
			{
				if(considerBoundaryIntersection)
				{
					result=strtree.query((Envelope)this.queryWindow);
				}
				else
				{
					List<Object> intermediateResult = strtree.query((Envelope)this.queryWindow);
					for(Object spatialObject:intermediateResult)
					{
						if(((Envelope)this.queryWindow).contains(((Geometry)spatialObject).getEnvelopeInternal()))
						{
							result.add(spatialObject);
						}
					}
				}
				
			}
			else
			{
				List tempResult=new ArrayList();
				tempResult=strtree.query(((Polygon)this.queryWindow).getEnvelopeInternal());
				for(Object spatialObject:tempResult)
				{
					if(considerBoundaryIntersection)
					{
						if(((Polygon)this.queryWindow).intersects((Geometry)spatialObject)) result.add(spatialObject);
					}
					else
					{
						if(((Polygon)this.queryWindow).covers((Geometry)spatialObject)) result.add(spatialObject);
					}
				}
			}
			return result;
		}
		else
		{
			Quadtree quadtree= (Quadtree) treeIndex;
			List result=new ArrayList();
			if(this.queryWindow instanceof Envelope)
			{
				result=quadtree.query((Envelope)this.queryWindow);
			}
			else
			{
				List tempResult=new ArrayList();
				tempResult=quadtree.query(((Polygon)this.queryWindow).getEnvelopeInternal());
				for(Object spatialObject:tempResult)
				{
					if(considerBoundaryIntersection)
					{
						if(((Polygon)this.queryWindow).intersects((Geometry)spatialObject)) result.add(spatialObject);
					}
					else
					{
						if(((Polygon)this.queryWindow).covers((Geometry)spatialObject)) result.add(spatialObject);
					}				}
			}
			return result;
		}
	}
}
