/**
 * FILE: PointByPolygonJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.joinJudgement.PointByPolygonJudgementUsingIndex.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PointByPolygonJudgementUsingIndex.
 */
public class PointByPolygonJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>>, Polygon, HashSet<Point>>, Serializable{
	
	/**
	 * Instantiates a new point by polygon judgement using index.
	 */
	public PointByPolygonJudgementUsingIndex()
	{
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
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
