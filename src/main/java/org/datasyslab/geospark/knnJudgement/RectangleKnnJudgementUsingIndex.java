package org.datasyslab.geospark.knnJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;

public class RectangleKnnJudgementUsingIndex implements FlatMapFunction<Iterator<STRtree>, Envelope>, Serializable{
	int k;
	Point queryCenter;
	public RectangleKnnJudgementUsingIndex(Point queryCenter,int k)
	{
		this.queryCenter=queryCenter;
		this.k=k;
	}
	@Override
	public Iterator<Envelope> call(Iterator<STRtree> t) throws Exception {
		// TODO Auto-generated method stub
		GeometryFactory fact= new GeometryFactory();
		STRtree strtree	=	t.next();
		Object[] localK = strtree.kNearestNeighbour(queryCenter.getEnvelopeInternal(), queryCenter, new GeometryItemDistance(), k);
		List<Envelope> result = new ArrayList<Envelope>();
		for(int i=0;i<localK.length;i++)
		{
			Geometry neighborPoly=(Geometry)localK[i];
			Envelope neighbor=new Envelope(neighborPoly.getEnvelopeInternal());
			neighbor.setUserData(neighborPoly.getUserData());
			result.add(neighbor);
		}
		
		return result.iterator();
	}
	
}
