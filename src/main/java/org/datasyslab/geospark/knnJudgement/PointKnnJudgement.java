package org.datasyslab.geospark.knnJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Point;



public class PointKnnJudgement implements FlatMapFunction<Iterator<Point>, Point>, Serializable{
	int k;
	Point queryCenter;
	public PointKnnJudgement(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
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
