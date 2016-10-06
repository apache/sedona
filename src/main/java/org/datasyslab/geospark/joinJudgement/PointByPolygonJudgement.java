package org.datasyslab.geospark.joinJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class PointByPolygonJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Polygon>>>, Polygon, HashSet<Point>>, Serializable{

	public PointByPolygonJudgement()
	{
	}

	
	@Override
	 public Iterator<Tuple2<Polygon, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Polygon>>> cogroup) throws Exception {
        ArrayList<Tuple2<Polygon, HashSet<Point>>> result = new ArrayList<Tuple2<Polygon, HashSet<Point>>>();

        Tuple2<Iterable<Point>, Iterable<Polygon>> cogroupTupleList = cogroup._2();
        for (Polygon e : cogroupTupleList._2()) {
            HashSet<Point> poinitHashSet = new HashSet<Point>();
            for (Point p : cogroupTupleList._1()) {
                if (e.contains(p)) {
                    poinitHashSet.add(p);
                }
            }
            result.add(new Tuple2<Polygon, HashSet<Point>>(e, poinitHashSet));
        }
        return result.iterator();
    }

}
