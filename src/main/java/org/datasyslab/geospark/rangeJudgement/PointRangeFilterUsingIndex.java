package org.datasyslab.geospark.rangeJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;

public class PointRangeFilterUsingIndex implements FlatMapFunction<Iterator<STRtree>,Point>, Serializable{

	Envelope rangeRectangle=new Envelope();
	
	public PointRangeFilterUsingIndex(Envelope envelope)
	{
		this.rangeRectangle=envelope;

	}
	@Override
	public Iterator<Point> call(Iterator<STRtree> t) throws Exception {
		// TODO Auto-generated method stub
		assert t.hasNext()==true;
		STRtree strtree= t.next();
		List<Point> result=new ArrayList<Point>();
		result=strtree.query(this.rangeRectangle);
		return result.iterator();
	}

}
