/**
 * FILE: PointByRectangleJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.PointByRectangleJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PointByRectangleJudgement.
 */
public class PointByRectangleJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Envelope>>>, Envelope, HashSet<Point>>, Serializable{
	
	/** The grid number. */
	int gridNumber;
	
	/**
	 * Instantiates a new point by rectangle judgement.
	 *
	 * @param gridNumber the grid number
	 */
	public PointByRectangleJudgement(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	 public Iterator<Tuple2<Envelope, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Envelope>>> cogroup) throws Exception {
		HashSet<Tuple2<Envelope, HashSet<Point>>> result = new HashSet<Tuple2<Envelope, HashSet<Point>>>();
		if(cogroup._1()>=gridNumber)
		{
			//Ok. We found this partition contains missing objects. Lets ignore this part.
			//return result;
		}
        Iterator<Envelope> iteratorWindow=cogroup._2()._2().iterator();
        while(iteratorWindow.hasNext()) {
        	Envelope window=iteratorWindow.next();
            HashSet<Point> poinitHashSet = new HashSet<Point>();
            Iterator<Point> iteratorObject=cogroup._2()._1().iterator();
            while (iteratorObject.hasNext()) {
            	Point object = iteratorObject.next();
                if (window.contains(object.getCoordinate())) {
                    poinitHashSet.add(object);
                }
            }
            result.add(new Tuple2<Envelope, HashSet<Point>>(window, poinitHashSet));
        }
        return result.iterator();
    }

}
