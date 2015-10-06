package org.datasyslab.geospark.boundryFilter;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Created by jinxuanw on 10/6/15.
 */
public class PolygonPreFilter  implements Function<Polygon,Boolean>,Serializable
{
	Envelope boundary;
	public PolygonPreFilter(Envelope boundary)
	{
		this.boundary=boundary;
	}
	public Boolean call(Polygon v1){
		if(boundary.contains(v1.getEnvelopeInternal())||boundary.intersects(v1.getEnvelopeInternal())||v1.getEnvelopeInternal().contains(boundary))
		{
			return true;
		}
		else return false;
	}
}
