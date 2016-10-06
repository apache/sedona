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
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

public class PolygonRangeFilterUsingIndex implements FlatMapFunction<Iterator<STRtree>,Polygon>, Serializable{
	Envelope rangeRectangle=new Envelope();
	
	public PolygonRangeFilterUsingIndex(Envelope envelope)
	{
		this.rangeRectangle=envelope;

	}
	@Override
	public Iterator<Polygon> call(Iterator<STRtree> t) throws Exception {
		// TODO Auto-generated method stub
		assert t.hasNext()==true;
		STRtree strtree= t.next();
		List<Polygon> result=new ArrayList<Polygon>();
		result=strtree.query(this.rangeRectangle);
		return result.iterator();
	}

}
