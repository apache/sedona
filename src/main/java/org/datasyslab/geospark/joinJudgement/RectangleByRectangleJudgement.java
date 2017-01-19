/**
 * FILE: RectangleByRectangleJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.RectangleByRectangleJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleByRectangleJudgement.
 */
public class RectangleByRectangleJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>>, Envelope, HashSet<Envelope>>, Serializable{

	/** The grid number. */
	int gridNumber;
	
	/**
	 * Instantiates a new rectangle by rectangle judgement.
	 *
	 * @param gridNumber the grid number
	 */
	public RectangleByRectangleJudgement(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	 public Iterator<Tuple2<Envelope, HashSet<Envelope>>> call(Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroup) throws Exception {
		HashSet<Tuple2<Envelope, HashSet<Envelope>>> result = new HashSet<Tuple2<Envelope, HashSet<Envelope>>>();
        Iterator iteratorWindow=cogroup._2()._2().iterator();
        while(iteratorWindow.hasNext()) {
        	Envelope window=(Envelope)iteratorWindow.next();
            HashSet<Envelope> objectHashSet = new HashSet<Envelope>();
            Iterator iteratorObject=cogroup._2()._1().iterator();
            while (iteratorObject.hasNext()) {
            	Envelope object = (Envelope)iteratorObject.next();
                if (window.contains(object)) {
                	objectHashSet.add(object);
                }
            }
            result.add(new Tuple2<Envelope, HashSet<Envelope>>(window, objectHashSet));
        }
        return result.iterator();
    }

}
