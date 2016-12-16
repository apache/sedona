/**
 * FILE: RectangleKnnJudgement.java
 * PATH: org.datasyslab.geospark.knnJudgement.RectangleKnnJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.knnJudgement;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;



// TODO: Auto-generated Javadoc
/**
 * The Class RectangleKnnJudgement.
 */
public class RectangleKnnJudgement implements FlatMapFunction<Iterator<Envelope>, Envelope>, Serializable{
	
	/** The k. */
	int k;
	
	/** The query center. */
	Point queryCenter;
	
	/**
	 * Instantiates a new rectangle knn judgement.
	 *
	 * @param queryCenter the query center
	 * @param k the k
	 */
	public RectangleKnnJudgement(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Envelope> call(Iterator<Envelope> input) throws Exception {
		// TODO Auto-generated method stub
		
		PriorityQueue<Envelope> pq = new PriorityQueue<Envelope>(k, new RectangleDistanceComparator(queryCenter));
		while (input.hasNext()) {
			if (pq.size() < k) {
				pq.offer(input.next());
			} else {
				Envelope curpoint = input.next();
				double distance = curpoint.distance(queryCenter.getEnvelopeInternal());
				double largestDistanceInPriQueue = pq.peek().distance(queryCenter.getEnvelopeInternal());
				if (largestDistanceInPriQueue > distance) {
					pq.poll();
					pq.offer(curpoint);
				}
			}
		}

		HashSet<Envelope> res = new HashSet<Envelope>();
		for (int i = 0; i < k; i++) {
			res.add(pq.poll());
		}
		return res.iterator();
	}

}
