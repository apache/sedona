package org.datasyslab.geospark.boundryFilter;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Created by jinxuanw on 10/6/15.
 */
public class PointPreFilter implements Function<Point,Boolean>,Serializable
{
	Envelope boundary;
	public PointPreFilter(Envelope boundary)
	{
		this.boundary=boundary;
	}
	public Boolean call(Point v1){
		if(boundary.contains(v1.getCoordinate()))
		{
			return true;
		}
		else return false;
	}

}
