/**
 * FILE: GeometryByPolygonJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
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
            Iterator iteratorObject=cogroup._2()._1().iterator();
            while (iteratorObject.hasNext()) {
            	Geometry object = (Geometry)iteratorObject.next();
                if (window.contains(object)) {
                	resultHashSet.add(object);
                }
            }
            result.add(new Tuple2<Polygon, HashSet<Geometry>>(window, resultHashSet));
        }
        return result.iterator();
    }

}
