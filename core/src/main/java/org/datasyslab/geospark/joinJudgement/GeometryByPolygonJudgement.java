/**
 * FILE: GeometryByPolygonJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgement.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class GeometryByPolygonJudgement.
 */
public class GeometryByPolygonJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>>, Polygon, HashSet<Geometry>>, Serializable{
	
	/** The consider boundary intersection. */
	boolean considerBoundaryIntersection=false;
	
	/**
	 * Instantiates a new geometry by polygon judgement.
	 *
	 * @param considerBoundaryIntersection the consider boundary intersection
	 */
	public GeometryByPolygonJudgement(boolean considerBoundaryIntersection)
	{
		this.considerBoundaryIntersection = considerBoundaryIntersection;
	}
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	 public Iterator<Tuple2<Polygon, HashSet<Geometry>>> call(Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Geometry>>> result = new HashSet<Tuple2<Polygon, HashSet<Geometry>>>();
        Iterator iteratorWindow=cogroup._2()._2().iterator();
        while(iteratorWindow.hasNext()) {
        	Polygon window=(Polygon)iteratorWindow.next();
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
            result.add(new Tuple2<Polygon, HashSet<Geometry>>(window, resultHashSet));
        }
        return result.iterator();
    }

}
