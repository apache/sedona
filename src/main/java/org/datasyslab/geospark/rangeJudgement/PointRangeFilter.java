package org.datasyslab.geospark.rangeJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class PointRangeFilter implements Function<Point,Boolean>,Serializable {
	Integer condition=0;
	Double x1,y1,x2,y2;
	Envelope rangeRectangle=new Envelope();
	Polygon rangePolygon;
	Integer rangeFlag=0;;
	public PointRangeFilter(Envelope envelope,Integer condition)
	{
		this.condition=condition;
		this.rangeRectangle=envelope;
		this.rangeFlag=0;
	}
	
	
	
//	public PointRangeFilter(Polygon polygon,Integer condition)
//	{
//		this.condition=condition;
//		this.rangePolygon=polygon;
//		this.rangeFlag=1;
//	}
	public Boolean call(Point tuple) throws Exception {
		if(rangeFlag==0){
			if(condition==0)
			{
				if(rangeRectangle.contains(tuple.getCoordinate()))
				{
					return true;
				}
				else return false;
			}
			else
			{
				if(rangeRectangle.intersects(tuple.getCoordinate()))
				{
					return true;
				}
				else return false; 
			}
		}
		//todo:  fix later;
		else {
			return false;
		}
//	else
//	{
//		if(condition==0)
//		{
//			if(rangePolygon.contains(tuple))
//			{
//				return true;
//			}
//			else return false;
//		}
//		else
//		{
//			if(rangePolygon.intersects(tuple))
//			{
//				return true;
//			}
//			else return false;
//		}
//	}
	}
}
