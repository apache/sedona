package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.datasyslab.geospark.boundryFilter.CircleFilterPoint;
import org.datasyslab.geospark.boundryFilter.PointPreFilter;
import org.datasyslab.geospark.boundryFilter.PolygonPreFilter;
import org.datasyslab.geospark.gemotryObjects.Circle;
import org.datasyslab.geospark.partition.PartitionAssignGridCircle;
import org.datasyslab.geospark.partition.PartitionAssignGridPoint;
import org.datasyslab.geospark.partition.PartitionAssignGridPolygon;
import org.datasyslab.geospark.partition.PartitionAssignGridRectangle;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialPairRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

/**
 * Spatial knn query.
 *
 * @param p
 *                        the p
 * @param k
 *                        the k
 * @return the list
 */
public class KNNQuery {
	public static List<Point> SpatialKnnQuery(PointRDD pointRDD, final Broadcast<Point> p, final Integer k) {
		// For each partation, build a priority queue that holds the topk
		@SuppressWarnings("serial")
		class PointCmp implements Comparator<Point>, Serializable {

			public int compare(Point p1, Point p2) {
				// TODO Auto-generated method stub
				double distance1 = p1.getCoordinate().distance(p.value().getCoordinate());
				double distance2 = p2.getCoordinate().distance(p.value().getCoordinate());
				if (distance1 > distance2) {
					return 1;
				} else if (distance1 == distance2) {
					return 0;
				}
				return -1;
			}

		}
		final PointCmp pcmp = new PointCmp();

		JavaRDD<Point> tmp = pointRDD.getPointRDD().mapPartitions(new FlatMapFunction<Iterator<Point>, Point>() {

			public Iterable<Point> call(Iterator<Point> input) throws Exception {
				PriorityQueue<Point> pq = new PriorityQueue<Point>(k, pcmp);
				while (input.hasNext()) {
					if (pq.size() < k) {
						pq.offer(input.next());
					} else {
						Point curpoint = input.next();
						double distance = curpoint.getCoordinate().distance(p.getValue().getCoordinate());
						double largestDistanceInPriQueue = pq.peek().getCoordinate()
								.distance(p.value().getCoordinate());
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
				// return is what?
				return res;
			}
		});

		// Take the top k

		return tmp.takeOrdered(k, pcmp);

	}
}
