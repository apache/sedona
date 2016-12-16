/**
 * FILE: PolygonKnnJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.knnJudgement.PolygonKnnJudgementUsingIndex.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;



// TODO: Auto-generated Javadoc
/**
 * The Class PolygonKnnJudgementUsingIndex.
 */
public class PolygonKnnJudgementUsingIndex implements FlatMapFunction<Iterator<STRtree>, Polygon>, Serializable{
	
	/** The k. */
	int k;
	
	/** The query center. */
	Point queryCenter;
	
	/**
	 * Instantiates a new polygon knn judgement using index.
	 *
	 * @param queryCenter the query center
	 * @param k the k
	 */
	public PolygonKnnJudgementUsingIndex(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Polygon> call(Iterator<STRtree> t) throws Exception {
		// TODO Auto-generated method stub
		GeometryFactory fact= new GeometryFactory();
		STRtree strtree	=	t.next();
		Object[] localK = strtree.kNearestNeighbour(queryCenter.getEnvelopeInternal(), queryCenter, new GeometryItemDistance(), k);
		List<Polygon> result = new ArrayList<Polygon>();
		for(int i=0;i<localK.length;i++)
		{
			result.add((Polygon)localK[i]);
		}
		
		return result.iterator();
	}

}
