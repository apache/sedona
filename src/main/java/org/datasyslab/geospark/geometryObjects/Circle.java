/**
 * FILE: Circle.java
 * PATH: org.datasyslab.geospark.geometryObjects.Circle.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.geometryObjects;

import java.io.Serializable;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;


// TODO: Auto-generated Javadoc
/**
 * The Class Circle.
 */
public class Circle implements Serializable {
	
	/** The center. */
	private Point center;
	
	/** The radius. */
	private Double radius;
	
	/**
	 * Instantiates a new circle.
	 *
	 * @param center the center
	 * @param radius the radius
	 */
	public Circle(Point center, Double radius)
	{
		this.center = center;
		this.radius=radius;
	}
	
	/**
	 * Instantiates a new circle.
	 *
	 * @param x the x
	 * @param y the y
	 * @param radius the radius
	 */
	public Circle(Double x,Double y,Double radius)
	{
		GeometryFactory fact = new GeometryFactory();
		Coordinate coordinate = new Coordinate(x,y);
		Point point=fact.createPoint(coordinate);
		this.center =point;
		this.radius=radius;
	}
	
	/**
	 * Gets the center.
	 *
	 * @return the center
	 */
	public Point getCenter()
	{
		return this.center;
	}
	
	/**
	 * Gets the radius.
	 *
	 * @return the radius
	 */
	public Double getRadius() {
		return radius;
	}
	
	/**
	 * Sets the radius.
	 *
	 * @param radius the new radius
	 */
	public void setRadius(Double radius) {
		this.radius = radius;
	}
	
	/**
	 * Gets the mbr.
	 *
	 * @return the mbr
	 */
	public Envelope getMBR()
	{
		Envelope mbr=new Envelope(center.getX()-radius, center.getX()+radius, center.getY()-radius, center.getY()+radius);
		return mbr;
	}
	
	/**
	 * MB rto circle.
	 *
	 * @param mbr the mbr
	 * @return the circle
	 */
	public static Circle MBRtoCircle(Envelope mbr)
	{
		Double radius=(mbr.getMaxX() - mbr.getMinX())/2;
		GeometryFactory fact = new GeometryFactory();
		Coordinate coordinate = new Coordinate(mbr.getMinX()+radius,mbr.getMinY()+radius);
		Point point=fact.createPoint(coordinate);
		return new Circle(point,radius);
	}
	
	/**
	 * Contains.
	 *
	 * @param point the point
	 * @return true, if successful
	 */
	public boolean contains(Point point)
	{
		if(this.center.distance(point)<this.radius)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * Intersects.
	 *
	 * @param point the point
	 * @return true, if successful
	 */
	public boolean intersects(Point point)
	{
		if(this.center.distance(point)<=this.radius)
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	/**
	 * Intersects.
	 *
	 * @param e the e
	 * @return true, if successful
	 */
	//Judge whether
	public boolean intersects(Envelope e)
	{
		//todo: implement intersect.
		Double cx = this.center.getX();
		Double cy = this.center.getY();
		Double radius = this.radius;
		Double recx = (e.getMinX() + e.getMaxX()) / 2;
		Double rectwidth = e.getMaxX() - e.getMinX();
		Double rectheight = e.getMaxY() - e.getMinY();
		Double recy = (e.getMaxY() + e.getMinY()) / 2;

		Double circleDistancex = Math.abs(cx - recx);
		Double circleDistancey = Math.abs(cy - recy);

		if (circleDistancex > (rectwidth/2 + radius)) { return false; }
		if (circleDistancey > (rectheight/2 + radius)) { return false; }

		if (circleDistancex <= (rectwidth/2)) { return true; }
		if (circleDistancey <= (rectheight/2)) { return true; }

		Double cornerDistance_sq = (circleDistancex - rectwidth/2)*(circleDistancex - rectwidth/2) + (circleDistancey - rectheight/2)*(circleDistancey - rectheight/2);

		return (cornerDistance_sq <= (radius*radius));
	}
}
