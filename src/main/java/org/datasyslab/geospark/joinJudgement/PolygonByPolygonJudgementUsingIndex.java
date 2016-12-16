/**
 * FILE: PolygonByPolygonJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.joinJudgement.PolygonByPolygonJudgementUsingIndex.java
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

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PolygonByPolygonJudgementUsingIndex.
 */
public class PolygonByPolygonJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>>, Polygon, HashSet<Polygon>>, Serializable{

	/** The grid number. */
	int gridNumber;
	
	/**
	 * Instantiates a new polygon by polygon judgement using index.
	 *
	 * @param gridNumber the grid number
	 */
	public PolygonByPolygonJudgementUsingIndex(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
    public Iterator<Tuple2<Polygon, HashSet<Polygon>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Polygon>>> result = new HashSet<Tuple2<Polygon, HashSet<Polygon>>>();
		Iterator<Polygon> iteratorWindow=cogroup._2()._2().iterator();
        Iterator<STRtree> iteratorTree=cogroup._2()._1().iterator();
        if(!iteratorTree.hasNext())
        {
        	return result.iterator();
        }
        STRtree strtree = iteratorTree.next();
        while(iteratorWindow.hasNext()) {
        	Polygon window=iteratorWindow.next();  
        	List<Polygon> queryResult=new ArrayList<Polygon>();
            queryResult=strtree.query(window.getEnvelopeInternal());
            HashSet<Polygon> resultHashSet = new HashSet<Polygon>(queryResult);
            result.add(new Tuple2<Polygon, HashSet<Polygon>>(window, resultHashSet));    
        }
        return result.iterator();
    }

}
