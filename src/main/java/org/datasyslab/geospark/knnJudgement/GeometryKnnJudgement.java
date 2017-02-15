/**
 * FILE: GeometryKnnJudgement.java
 * PATH: org.datasyslab.geospark.knnJudgement.GeometryKnnJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;



// TODO: Auto-generated Javadoc
/**
 * The Class GeometryKnnJudgement.
 */
public class GeometryKnnJudgement implements FlatMapFunction<Iterator<Object>, Object>, Serializable{
	
	/** The k. */
	int k;
	
	/** The query center. */
	Point queryCenter;
	
	/**
	 * Instantiates a new geometry knn judgement.
	 *
	 * @param queryCenter the query center
	 * @param k the k
	 */
	public GeometryKnnJudgement(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Object> call(Iterator<Object> input) throws Exception {		
		PriorityQueue<Geometry> pq = new PriorityQueue<Geometry>(k, new GeometryDistanceComparator(queryCenter,false));
		while (input.hasNext()) {
			if (pq.size() < k) {
				pq.offer((Geometry)input.next());
			} else {
				Geometry curpoint = (Geometry)input.next();
				double distance = curpoint.distance(queryCenter);
				double largestDistanceInPriQueue = pq.peek().distance(queryCenter);
				if (largestDistanceInPriQueue > distance) {
					pq.poll();
					pq.offer(curpoint);
				}
			}
		}
		ArrayList res = new ArrayList<Point>();
		for (int i = 0; i < k; i++) {
			res.add(pq.poll());
		}
		return res.iterator();
	}

}
