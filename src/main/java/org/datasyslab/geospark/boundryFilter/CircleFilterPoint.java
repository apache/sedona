package org.datasyslab.geospark.boundryFilter;

import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jinxuanw on 10/5/15.
 */
public class CircleFilterPoint implements Serializable,Function<Tuple2<Point,Point>,Boolean>
{
	Double radius=0.0;
	Integer condition=1;
	public CircleFilterPoint(Double radius,Integer condition)
	{
		this.radius=radius;
		this.condition=condition;
	}

	public Boolean call(Tuple2<Point, Point> t){
		if(condition==0){
			if(t._1().distance(t._2())<radius)
			{
				return true;
			}
			else return false;
			}
			else
			{
				if(t._1().distance(t._2())<=radius)
				{
					return true;
				}
				return false;
			}
	}
}
