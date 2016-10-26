package org.datasyslab.geospark.rangeJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.strtree.STRtree;

public class RectangleRangeFilterUsingIndex implements FlatMapFunction<Iterator<STRtree>,Envelope>, Serializable{
	Envelope rangeRectangle=new Envelope();
	
	public RectangleRangeFilterUsingIndex(Envelope envelope)
	{
		this.rangeRectangle=envelope;

	}
	@Override
	public Iterator<Envelope> call(Iterator<STRtree> t) throws Exception {
		// TODO Auto-generated method stub
		assert t.hasNext()==true;
		STRtree strtree= t.next();
		List<Geometry> initialResult=new ArrayList<Geometry>();
		initialResult=strtree.query(this.rangeRectangle);
		HashSet<Envelope> result=new HashSet<Envelope>();
		Iterator<Geometry> initialResultIterator=initialResult.iterator();
		while(initialResultIterator.hasNext())
		{
			Geometry oneResultPoly=(Geometry)initialResultIterator.next();
			Envelope oneResult=oneResultPoly.getEnvelopeInternal();
			oneResult.setUserData(oneResultPoly.getUserData());
			result.add(oneResult);
			
		}
		return result.iterator();
	}

}
