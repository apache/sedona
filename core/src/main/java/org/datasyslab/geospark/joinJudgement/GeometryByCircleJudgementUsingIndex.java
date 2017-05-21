/**
 * FILE: GeometryByCircleJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByCircleJudgementUsingIndex.java
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
import org.datasyslab.geospark.geometryObjects.Circle;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class GeometryByCircleJudgementUsingIndex.
 */
public class GeometryByCircleJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<SpatialIndex>, Iterable<Object>>>, Circle, HashSet<Geometry>>, Serializable{
	
	/** The consider boundary intersection. */
	boolean considerBoundaryIntersection=false;
	
	/**
	 * Instantiates a new geometry by circle judgement using index.
	 *
	 * @param considerBoundaryIntersection the consider boundary intersection
	 */
	public GeometryByCircleJudgementUsingIndex(boolean considerBoundaryIntersection)
	{
		this.considerBoundaryIntersection = considerBoundaryIntersection;
	}
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
    public HashSet<Tuple2<Circle, HashSet<Geometry>>> call(Tuple2<Integer, Tuple2<Iterable<SpatialIndex>, Iterable<Object>>> cogroup) throws Exception {
		HashSet<Tuple2<Circle, HashSet<Geometry>>> result = new HashSet<Tuple2<Circle, HashSet<Geometry>>>();
		Iterator<Object> iteratorWindow=cogroup._2()._2().iterator();
        Iterator<SpatialIndex> iteratorTree=cogroup._2()._1().iterator();
        if(!iteratorTree.hasNext())
        {
        	return result;
        }
        SpatialIndex treeIndex = iteratorTree.next();
        if(treeIndex instanceof STRtree)
        {
        	treeIndex = (STRtree)treeIndex;
        }
        else
        {
        	treeIndex = (Quadtree)treeIndex;
        }
        while(iteratorWindow.hasNext()) {
        	Circle window=(Circle)iteratorWindow.next();  
            List<Geometry> queryResult=new ArrayList<Geometry>();
            queryResult=treeIndex.query(window.getEnvelopeInternal());
            if(queryResult.size()==0) continue;
            HashSet<Geometry> objectHashSet = new HashSet<Geometry>();
            for(Geometry spatialObject:queryResult)
            {
				// Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
            	if(considerBoundaryIntersection)
            	{
            		if (window.intersects(spatialObject)) {
            			objectHashSet.add(spatialObject);
            		}
            	}
            	else{
            		if (window.covers(spatialObject)) {
            			objectHashSet.add(spatialObject);
            		}
            	}
            }
            if(objectHashSet.size()==0) continue;
            result.add(new Tuple2<Circle, HashSet<Geometry>>(window, objectHashSet));   
        }
        return result;
    }
}
