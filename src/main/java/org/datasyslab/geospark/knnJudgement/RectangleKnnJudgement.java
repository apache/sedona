package org.datasyslab.geospark.knnJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;



public class RectangleKnnJudgement implements FlatMapFunction<Iterator<Envelope>, Envelope>, Serializable{
	int k;
	Point queryCenter;
	public RectangleKnnJudgement(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
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
