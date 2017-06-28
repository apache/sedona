/**
 * FILE: GeometryByCircleJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByCircleJudgement.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.datasyslab.geospark.geometryObjects.Circle;

import com.vividsolutions.jts.geom.Geometry;

import scala.Tuple2;


// TODO: Auto-generated Javadoc
/**
 * The Class GeometryByCircleJudgement.
 */
public class GeometryByCircleJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>>, Circle, HashSet<Geometry>>, Serializable{
	
	/** The consider boundary intersection. */
	boolean considerBoundaryIntersection=false;
	
	/**
	 * Instantiates a new geometry by circle judgement.
	 *
	 * @param considerBoundaryIntersection the consider boundary intersection
	 */
	public GeometryByCircleJudgement(boolean considerBoundaryIntersection)
	{
		this.considerBoundaryIntersection = considerBoundaryIntersection;
	}
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	 public Iterator<Tuple2<Circle, HashSet<Geometry>>> call(Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroup) throws Exception {
		HashSet<Tuple2<Circle, HashSet<Geometry>>> result = new HashSet<Tuple2<Circle, HashSet<Geometry>>>();
        Iterator iteratorWindow=cogroup._2()._2().iterator();
        while(iteratorWindow.hasNext()) {
        	Circle window=(Circle)iteratorWindow.next();
            HashSet<Geometry> resultHashSet = new HashSet<Geometry>();
            Iterator<Object> iteratorObject=cogroup._2()._1().iterator();
            while (iteratorObject.hasNext()) {
            	Geometry object = (Geometry) iteratorObject.next();
            	if(considerBoundaryIntersection)
            	{
            		if (window.intersects(object)) {
            			resultHashSet.add(object);
            		}
            	}
            	else{
            		if (window.covers(object)) {
            			resultHashSet.add(object);
            		}
            	}
            }
            if(resultHashSet.size()==0) continue;
            result.add(new Tuple2<Circle, HashSet<Geometry>>(window, resultHashSet));
        }
        return result.iterator();
    }

}
