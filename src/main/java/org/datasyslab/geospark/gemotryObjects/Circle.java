/*
 * 
 */
package GeoSpark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

// TODO: Auto-generated Javadoc
/**
 * The Class Circle.
 */
public class Circle implements Serializable {
	
	/** The centre. */
	private Point centre;
	
	/** The radius. */
	private Double radius;
	
	/**
	 * Instantiates a new circle.
	 *
	 * @param centre the centre
	 * @param radius the radius
	 */
	public Circle(Point centre,Double radius)
	{
		this.centre=centre;
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
		this.centre=point;
		this.radius=radius;
	}
	
	/**
	 * Gets the centre.
	 *
	 * @return the centre
	 */
	public Point getCentre()
	{
		return this.centre;
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
		Envelope mbr=new Envelope(centre.getX()-radius,centre.getX()+radius,centre.getY()-radius,centre.getY()+radius);
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
		Double radius=(mbr.getMinX()+mbr.getMaxX())/2;
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
		if(this.centre.distance(point)<this.radius)
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
		if(this.centre.distance(point)<=this.radius)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
}
