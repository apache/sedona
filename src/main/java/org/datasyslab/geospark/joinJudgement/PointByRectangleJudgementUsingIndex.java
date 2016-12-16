/**
 * FILE: PointByRectangleJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.joinJudgement.PointByRectangleJudgementUsingIndex.java
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
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PointByRectangleJudgementUsingIndex.
 */
public class PointByRectangleJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>>, Envelope, HashSet<Point>>, Serializable{

	/** The grid number. */
	int gridNumber;
	
	/**
	 * Instantiates a new point by rectangle judgement using index.
	 *
	 * @param gridNumber the grid number
	 */
	public PointByRectangleJudgementUsingIndex(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
    public Iterator<Tuple2<Envelope, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroup) throws Exception {
		HashSet<Tuple2<Envelope, HashSet<Point>>> result = new HashSet<Tuple2<Envelope, HashSet<Point>>>();
		if(cogroup._1()>=gridNumber)
		{
			//Ok. We found this partition contains missing objects. Lets ignore this part.
			//return result;
		}
		Iterator<Envelope> iteratorWindow=cogroup._2()._2().iterator();
        Iterator<STRtree> iteratorTree=cogroup._2()._1().iterator();
        if(!iteratorTree.hasNext())
        {
        	return result.iterator();
        }
        STRtree strtree = iteratorTree.next();
        while(iteratorWindow.hasNext()) {
        	Envelope window=iteratorWindow.next();  
            List<Point> queryResult=new ArrayList<Point>();
            queryResult=strtree.query(window);
            HashSet<Point> pointHashSet = new HashSet<Point>(queryResult);
            result.add(new Tuple2<Envelope, HashSet<Point>>(window, pointHashSet));   
        }
        return result.iterator();
    }

}
