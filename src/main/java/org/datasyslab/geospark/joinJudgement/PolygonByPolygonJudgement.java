/**
 * FILE: PolygonByPolygonJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.PolygonByPolygonJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonByPolygonJudgement.
 */
public class PolygonByPolygonJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Polygon>, Iterable<Polygon>>>, Polygon, HashSet<Polygon>>, Serializable{

	/** The grid number. */
	int gridNumber;
	
	/**
	 * Instantiates a new polygon by polygon judgement.
	 *
	 * @param gridNumber the grid number
	 */
	public PolygonByPolygonJudgement(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	 public Iterator<Tuple2<Polygon, HashSet<Polygon>>> call(Tuple2<Integer, Tuple2<Iterable<Polygon>, Iterable<Polygon>>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Polygon>>> result = new HashSet<Tuple2<Polygon, HashSet<Polygon>>>();
        Iterator<Polygon> iteratorWindow=cogroup._2()._2().iterator();
        while(iteratorWindow.hasNext()) {
        	Polygon window=iteratorWindow.next();
            HashSet<Polygon> objectHashSet = new HashSet<Polygon>();
            Iterator<Polygon> iteratorObject=cogroup._2()._1().iterator();
            while (iteratorObject.hasNext()) {
            	Polygon object = iteratorObject.next();
                if (window.contains(object)) {
                	objectHashSet.add(object);
                }
            }
            result.add(new Tuple2<Polygon, HashSet<Polygon>>(window, objectHashSet));
        }
        return result.iterator();
    }

}
