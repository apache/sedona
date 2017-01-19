/**
 * FILE: RangeFilterUsingIndex.java
 * PATH: org.datasyslab.geospark.rangeJudgement.RangeFilterUsingIndex.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

// TODO: Auto-generated Javadoc
/**
 * The Class RangeFilterUsingIndex.
 */
public class RangeFilterUsingIndex implements FlatMapFunction<Iterator<Object>,Object>, Serializable{

	/** The query window. */
	Object queryWindow;

	/**
	 * Instantiates a new range filter using index.
	 *
	 * @param queryWindow the query window
	 */
	public RangeFilterUsingIndex(Envelope queryWindow)
	{
		this.queryWindow=queryWindow;

	}
	
	/**
	 * Instantiates a new range filter using index.
	 *
	 * @param queryWindow the query window
	 */
	public RangeFilterUsingIndex(Polygon queryWindow)
	{
		this.queryWindow=queryWindow;

	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Object> call(Iterator<Object> treeIndexes) throws Exception {
		assert treeIndexes.hasNext()==true;
		Object treeIndex = treeIndexes.next();
		if(treeIndex instanceof STRtree)
		{
			STRtree strtree= (STRtree) treeIndex;
			List result=new ArrayList();
			if(this.queryWindow instanceof Envelope)
			{
				result=strtree.query((Envelope)this.queryWindow);
			}
			else
			{
				result=strtree.query(((Polygon)this.queryWindow).getEnvelopeInternal());
			}
			return result.iterator();
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
				result=quadtree.query(((Polygon)this.queryWindow).getEnvelopeInternal());
			}
			return result.iterator();
		}
	}
}
