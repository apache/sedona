/**
 * FILE: GeometryByRectangleJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByRectangleJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class GeometryByRectangleJudgement.
 */
public class GeometryByRectangleJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>>, Envelope, HashSet<Geometry>>, Serializable{
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	 public Iterator<Tuple2<Envelope, HashSet<Geometry>>> call(Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroup) throws Exception {
		HashSet<Tuple2<Envelope, HashSet<Geometry>>> result = new HashSet<Tuple2<Envelope, HashSet<Geometry>>>();
        Iterator iteratorWindow=cogroup._2()._2().iterator();
        while(iteratorWindow.hasNext()) {
        	Envelope window=(Envelope)iteratorWindow.next();
            HashSet<Geometry> resultHashSet = new HashSet<Geometry>();
            Iterator iteratorObject=cogroup._2()._1().iterator();
            while (iteratorObject.hasNext()) {
            	Geometry object = (Geometry)iteratorObject.next();
                if (window.contains(object.getEnvelopeInternal())) {
                	resultHashSet.add(object);
                }
            }
            result.add(new Tuple2<Envelope, HashSet<Geometry>>(window, resultHashSet));
        }
        return result.iterator();
    }

}
