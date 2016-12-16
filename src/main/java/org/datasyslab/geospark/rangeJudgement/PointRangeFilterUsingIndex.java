/**
 * FILE: PointRangeFilterUsingIndex.java
 * PATH: org.datasyslab.geospark.rangeJudgement.PointRangeFilterUsingIndex.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;

// TODO: Auto-generated Javadoc
/**
 * The Class PointRangeFilterUsingIndex.
 */
public class PointRangeFilterUsingIndex implements FlatMapFunction<Iterator<STRtree>,Point>, Serializable{

	/** The range rectangle. */
	Envelope rangeRectangle=new Envelope();
	
	/**
	 * Instantiates a new point range filter using index.
	 *
	 * @param envelope the envelope
	 */
	public PointRangeFilterUsingIndex(Envelope envelope)
	{
		this.rangeRectangle=envelope;

	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Point> call(Iterator<STRtree> t) throws Exception {
		// TODO Auto-generated method stub
		assert t.hasNext()==true;
		STRtree strtree= t.next();
		List<Point> result=new ArrayList<Point>();
		result=strtree.query(this.rangeRectangle);
		return result.iterator();
	}

}
