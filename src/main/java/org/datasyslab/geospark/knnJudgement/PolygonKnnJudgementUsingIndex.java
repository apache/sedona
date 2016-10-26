package org.datasyslab.geospark.knnJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;



public class PolygonKnnJudgementUsingIndex implements FlatMapFunction<Iterator<STRtree>, Polygon>, Serializable{
	int k;
	Point queryCenter;
	public PolygonKnnJudgementUsingIndex(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
	@Override
	public Iterator<Polygon> call(Iterator<STRtree> t) throws Exception {
		// TODO Auto-generated method stub
		GeometryFactory fact= new GeometryFactory();
		STRtree strtree	=	t.next();
		Object[] localK = strtree.kNearestNeighbour(queryCenter.getEnvelopeInternal(), queryCenter, new GeometryItemDistance(), k);
		List<Polygon> result = new ArrayList<Polygon>();
		for(int i=0;i<localK.length;i++)
		{
			result.add((Polygon)localK[i]);
		}
		
		return result.iterator();
	}

}
