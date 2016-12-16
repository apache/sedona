/**
 * FILE: PointByPolygonJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.PointByPolygonJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PointByPolygonJudgement.
 */
public class PointByPolygonJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Polygon>>>, Polygon, HashSet<Point>>, Serializable{

	/**
	 * Instantiates a new point by polygon judgement.
	 */
	public PointByPolygonJudgement()
	{
	}

	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
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
