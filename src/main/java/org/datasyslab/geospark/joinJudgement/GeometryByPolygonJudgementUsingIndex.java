/**
 * FILE: GeometryByPolygonJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgementUsingIndex.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class GeometryByPolygonJudgementUsingIndex.
 */
public class GeometryByPolygonJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>>, Polygon, HashSet<Geometry>>, Serializable{
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
    public Iterator<Tuple2<Polygon, HashSet<Geometry>>> call(Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Geometry>>> result = new HashSet<Tuple2<Polygon, HashSet<Geometry>>>();
		Iterator<Object> iteratorWindow=cogroup._2()._2().iterator();
        Iterator<Object> iteratorTree=cogroup._2()._1().iterator();
        if(!iteratorTree.hasNext())
        {
        	return result.iterator();
        }
        SpatialIndex treeIndex = (SpatialIndex) iteratorTree.next();
        if(treeIndex instanceof STRtree)
        {
        	treeIndex = (STRtree)treeIndex;
        }
        else
        {
        	treeIndex = (Quadtree)treeIndex;
        }
        while(iteratorWindow.hasNext()) {
        	Polygon window=(Polygon)iteratorWindow.next();  
            List<Geometry> queryResult=new ArrayList<Geometry>();
            queryResult=treeIndex.query(window.getEnvelopeInternal());
            HashSet<Geometry> objectHashSet = new HashSet<Geometry>(queryResult);
            result.add(new Tuple2<Polygon, HashSet<Geometry>>(window, objectHashSet));   
        }
        return result.iterator();
    }
}
