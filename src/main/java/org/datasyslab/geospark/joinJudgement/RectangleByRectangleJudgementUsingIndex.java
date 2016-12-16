/**
 * FILE: RectangleByRectangleJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.joinJudgement.RectangleByRectangleJudgementUsingIndex.java
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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class RectangleByRectangleJudgementUsingIndex.
 */
public class RectangleByRectangleJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>>, Envelope, HashSet<Envelope>>, Serializable{

	/** The grid number. */
	int gridNumber;
	
	/**
	 * Instantiates a new rectangle by rectangle judgement using index.
	 *
	 * @param gridNumber the grid number
	 */
	public RectangleByRectangleJudgementUsingIndex(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
    public Iterator<Tuple2<Envelope, HashSet<Envelope>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroup) throws Exception {
		HashSet<Tuple2<Envelope, HashSet<Envelope>>> result = new HashSet<Tuple2<Envelope, HashSet<Envelope>>>();
		Iterator<Envelope> iteratorWindow=cogroup._2()._2().iterator();
        Iterator<STRtree> iteratorTree=cogroup._2()._1().iterator();
        if(!iteratorTree.hasNext())
        {
        	return result.iterator();
        }
        STRtree strtree = iteratorTree.next();
        while(iteratorWindow.hasNext()) {
        	Envelope window=iteratorWindow.next();  
        	List<Envelope> queryResult=new ArrayList<Envelope>();
            queryResult=strtree.query(window);
            HashSet<Envelope> resultHashSet = new HashSet<Envelope>(queryResult);
            result.add(new Tuple2<Envelope, HashSet<Envelope>>(window, resultHashSet));    
        }
        return result.iterator();
    }

}
