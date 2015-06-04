package GeoSpark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class Circle implements Serializable {
	private Point centre;
	private Double radius;
	public Circle(Point centre,Double radius)
	{
		this.centre=centre;
		this.radius=radius;
	}
	public Circle(Double x,Double y,Double radius)
	{
		GeometryFactory fact = new GeometryFactory();
		Coordinate coordinate = new Coordinate(x,y);
		Point point=fact.createPoint(coordinate);
		this.centre=point;
		this.radius=radius;
	}
	public Point getCentre()
	{
		return this.centre;
	}
	public Double getRadius() {
		return radius;
	}
	public void setRadius(Double radius) {
		this.radius = radius;
	}
	public Envelope getMBR()
	{
		Envelope mbr=new Envelope(centre.getCoordinate().x-radius,centre.getCoordinate().y-radius,centre.getCoordinate().x+radius,centre.getCoordinate().y+radius);
		return mbr;
	}
	public static Circle MBRtoCircle(Envelope mbr)
	{
		Double radius=(mbr.getMinX()+mbr.getMaxX())/2;
		GeometryFactory fact = new GeometryFactory();
		Coordinate coordinate = new Coordinate(mbr.getMinX()+radius,mbr.getMinY()+radius);
		Point point=fact.createPoint(coordinate);
		return new Circle(point,radius);
	}
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