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
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

public class PointByPolygonJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>>, Polygon, HashSet<Point>>, Serializable{
	public PointByPolygonJudgementUsingIndex()
	{
	}
	@Override
    public Iterator<Tuple2<Polygon, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Point>>> result = new HashSet<Tuple2<Polygon, HashSet<Point>>>();

        Tuple2<Iterable<STRtree>, Iterable<Polygon>> cogroupTupleList = cogroup._2();
        for(Polygon e : cogroupTupleList._2()) {
            List<Point> pointList = new ArrayList<Point>();
            for(STRtree s:cogroupTupleList._1()) {
                pointList = s.query(e.getEnvelopeInternal());
            }
            HashSet<Point> pointSet = new HashSet<Point>(pointList);
            result.add(new Tuple2<Polygon, HashSet<Point>>(e, pointSet));
        }
        return result.iterator();
    }

}
