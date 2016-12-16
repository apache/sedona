/**
 * FILE: PointKnnJudgement.java
 * PATH: org.datasyslab.geospark.knnJudgement.PointKnnJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Point;



// TODO: Auto-generated Javadoc
/**
 * The Class PointKnnJudgement.
 */
public class PointKnnJudgement implements FlatMapFunction<Iterator<Point>, Point>, Serializable{
	
	/** The k. */
	int k;
	
	/** The query center. */
	Point queryCenter;
	
	/**
	 * Instantiates a new point knn judgement.
	 *
	 * @param queryCenter the query center
	 * @param k the k
	 */
	public PointKnnJudgement(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Point> call(Iterator<Point> input) throws Exception {
		// TODO Auto-generated method stub
		
		PriorityQueue<Point> pq = new PriorityQueue<Point>(k, new PointDistanceComparator(queryCenter));
		while (input.hasNext()) {
			if (pq.size() < k) {
				pq.offer(input.next());
			} else {
				Point curpoint = input.next();
				double distance = curpoint.getCoordinate().distance(queryCenter.getCoordinate());
				double largestDistanceInPriQueue = pq.peek().getCoordinate()
						.distance(queryCenter.getCoordinate());
				if (largestDistanceInPriQueue > distance) {
					pq.poll();
					pq.offer(curpoint);
				}
			}
		}

		ArrayList<Point> res = new ArrayList<Point>();
		for (int i = 0; i < k; i++) {
			res.add(pq.poll());
		}
		return res.iterator();
	}

}
