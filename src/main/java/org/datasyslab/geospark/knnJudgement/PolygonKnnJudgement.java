/**
 * FILE: PolygonKnnJudgement.java
 * PATH: org.datasyslab.geospark.knnJudgement.PolygonKnnJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;



// TODO: Auto-generated Javadoc
/**
 * The Class PolygonKnnJudgement.
 */
public class PolygonKnnJudgement implements FlatMapFunction<Iterator<Polygon>, Polygon>, Serializable{
	
	/** The k. */
	int k;
	
	/** The query center. */
	Point queryCenter;
	
	/**
	 * Instantiates a new polygon knn judgement.
	 *
	 * @param queryCenter the query center
	 * @param k the k
	 */
	public PolygonKnnJudgement(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Polygon> call(Iterator<Polygon> input) throws Exception {
		// TODO Auto-generated method stub
		
		PriorityQueue<Polygon> pq = new PriorityQueue<Polygon>(k, new PolygonDistanceComparator(queryCenter));
		while (input.hasNext()) {
			if (pq.size() < k) {
				pq.offer(input.next());
			} else {
				Polygon curpoint = input.next();
				double distance = curpoint.distance(queryCenter);
				double largestDistanceInPriQueue = pq.peek().distance(queryCenter);
				if (largestDistanceInPriQueue > distance) {
					pq.poll();
					pq.offer(curpoint);
				}
			}
		}

		HashSet<Polygon> res = new HashSet<Polygon>();
		for (int i = 0; i < k; i++) {
			res.add(pq.poll());
		}
		return res.iterator();
	}

}
