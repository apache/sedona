package org.datasyslab.geospark.boundryFilter;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.gemotryObjects.Circle;

import java.io.Serializable;

/**
 * Created by jinxuanw on 10/6/15.
 */
public class CirclePreFilter implements Function<Circle,Boolean>,Serializable
{
	Envelope boundary;
	public CirclePreFilter(Envelope boundary)
	{
		this.boundary=boundary;
	}
	public Boolean call(Circle v1){

		if(boundary.contains(v1.getMBR())||boundary.intersects(v1.getMBR())||v1.getMBR().contains(boundary))
		{
			return true;
		}
		else return false;
	}

}
