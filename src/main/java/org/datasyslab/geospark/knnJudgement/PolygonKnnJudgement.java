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
import com.vividsolutions.jts.geom.Polygon;



public class PolygonKnnJudgement implements FlatMapFunction<Iterator<Polygon>, Polygon>, Serializable{
	int k;
	Point queryCenter;
	public PolygonKnnJudgement(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
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
