/**
 * FILE: KnnJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.knnJudgement.KnnJudgementUsingIndex.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
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
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;

// TODO: Auto-generated Javadoc
/**
 * The Class KnnJudgementUsingIndex.
 */
public class KnnJudgementUsingIndex implements FlatMapFunction<Iterator<Object>, Object>, Serializable{
	
	/** The k. */
	int k;
	
	/** The query center. */
	Point queryCenter;
	
	/**
	 * Instantiates a new knn judgement using index.
	 *
	 * @param queryCenter the query center
	 * @param k the k
	 */
	public KnnJudgementUsingIndex(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Object> call(Iterator<Object> treeIndexes) throws Exception {
		// TODO Auto-generated method stub
		GeometryFactory fact= new GeometryFactory();
		Object treeIndex = treeIndexes.next();
		Object[] localK=null;
		if(treeIndex instanceof STRtree)
		{
			localK = ((STRtree)treeIndex).kNearestNeighbour(queryCenter.getEnvelopeInternal(), queryCenter, new GeometryItemDistance(), k);
		}
		else
		{
			throw new Exception("[KnnJudgementUsingIndex][Call] QuadTree index doesn't support KNN search.");
		}
		List result = new ArrayList();
		for(int i=0;i<localK.length;i++)
		{
			result.add(localK[i]);
		}
		return result.iterator();
	}
	
}
