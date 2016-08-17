package org.datasyslab.geospark.knnJudgement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
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
	public Iterable<Envelope> call(Iterator<STRtree> t) throws Exception {
		// TODO Auto-generated method stub
		GeometryFactory fact= new GeometryFactory();
		STRtree strtree	=	t.next();
		Envelope[] localK = strtree.kNearestNeighbour(queryCenter.getEnvelopeInternal(), queryCenter, new GeometryItemDistance(), k);
		return Arrays.asList(localK);
	}
	
}
