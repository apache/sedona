package org.datasyslab.geospark.geometryObjects;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;


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
	 * Gets the Minimum Boundring Rectangle.
	 *
	 * @return the Minimum Boundring Rectangle
	 */
	public Envelope getMBR()
	{
		Envelope mbr=new Envelope(center.getX()-radius, center.getX()+radius, center.getY()-radius, center.getY()+radius);
		return mbr;
	}
	
	/**
	 * Convert A Minimum Bounding Rectangle to a circle
	 *
	 * @param mbr the Minimum Boundring Rectangle
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
	 * Check whether the circle contains a point.
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
	 * Check whether the circle intersects a point. This one considers the case that the point is on the circle boundary.
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
