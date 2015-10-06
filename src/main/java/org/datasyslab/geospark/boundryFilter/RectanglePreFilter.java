package org.datasyslab.geospark.boundryFilter;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Created by jinxuanw on 10/6/15.
 */
public class RectanglePreFilter implements Function<Envelope,Boolean>,Serializable
{
	Envelope boundary;
	public RectanglePreFilter(Envelope boundary)
	{
		this.boundary=boundary;
	}
	public Boolean call(Envelope v1){
		if(boundary.contains(v1)||boundary.intersects(v1)||v1.contains(boundary))
		{
			return true;
		}
		else return false;
	}
}
