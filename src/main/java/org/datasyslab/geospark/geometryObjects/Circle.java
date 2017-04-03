/**
 * FILE: Circle.java
 * PATH: org.datasyslab.geospark.geometryObjects.Circle.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.geometryObjects;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.CoordinateSequenceComparator;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryComponentFilter;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.GeometryFilter;

/**
 * The Class Circle.
 */
public class Circle extends Geometry {
	
	/** The center. */
	private Geometry centerGeometry;
	
	
	/**
	 * Gets the center geometry.
	 *
	 * @return the center geometry
	 */
	public Geometry getCenterGeometry() {
		return centerGeometry;
	}

	/** The center point. */
	private Coordinate centerPoint;
	
	/** The radius. */
	private Double radius;
	
	/** The mbr. */
	private Envelope MBR;
	
	/**
	 * Instantiates a new circle.
	 *
	 * @param centerGeometry the center geometry
	 * @param givenRadius the given radius
	 */
	public Circle(Geometry centerGeometry, Double givenRadius)
	{
		super(new GeometryFactory(centerGeometry.getPrecisionModel()));
		this.centerGeometry = centerGeometry;		
		Envelope centerGeometryMBR = this.centerGeometry.getEnvelopeInternal();
		this.centerPoint = new Coordinate((centerGeometryMBR.getMinX()+centerGeometryMBR.getMaxX())/2.0,
				(centerGeometryMBR.getMinY()+centerGeometryMBR.getMaxY())/2.0);
		// Get the internal radius of the object. We need to make sure that the circle at least should be the minimum circumscribed circle
		double width = centerGeometryMBR.getMaxX()-centerGeometryMBR.getMinX();
		double length = centerGeometryMBR.getMaxY()-centerGeometryMBR.getMinY();
		double centerGeometryInternalRadius = Math.sqrt(width*width+length*length)/2;
		this.radius=givenRadius>centerGeometryInternalRadius?givenRadius:centerGeometryInternalRadius;
		this.MBR=new Envelope(this.centerPoint.x-this.radius, this.centerPoint.x+this.radius, this.centerPoint.y-this.radius, this.centerPoint.y+this.radius);
		this.setUserData(centerGeometry.getUserData());
	}
	

	/**
	 * Gets the center point.
	 *
	 * @return the center point
	 */
	public Coordinate getCenterPoint()
	{
		return this.centerPoint;
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
	 * @param givenRadius the new radius
	 */
	public void setRadius(Double givenRadius) {
		// Get the internal radius of the object. We need to make sure that the circle at least should be the minimum circumscribed circle
		Envelope centerGeometryMBR = this.centerGeometry.getEnvelopeInternal();
		double width = centerGeometryMBR.getMaxX()-centerGeometryMBR.getMinX();
		double length = centerGeometryMBR.getMaxY()-centerGeometryMBR.getMinY();
		double centerGeometryInternalRadius = Math.sqrt(width*width+length*length)/2;
		this.radius=givenRadius>centerGeometryInternalRadius?givenRadius:centerGeometryInternalRadius;
		this.MBR=new Envelope(this.centerPoint.x-this.radius, this.centerPoint.x+this.radius, this.centerPoint.y-this.radius, this.centerPoint.y+this.radius);
	}
	
	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#covers(com.vividsolutions.jts.geom.Geometry)
	 */
	@Override
	public boolean covers(Geometry g) {
		// short-circuit test
		if (! getEnvelopeInternal().covers(g.getEnvelopeInternal()))
			return false;
		// optimization for rectangle arguments
		if (isRectangle()) {
			// since we have already tested that the test envelope is covered
			return true;
		}
		double x1,x2,y1,y2;
		x1=g.getEnvelopeInternal().getMinX();
		x2=g.getEnvelopeInternal().getMaxX();
		y1=g.getEnvelopeInternal().getMinY();
		y2=g.getEnvelopeInternal().getMaxY();	
		return covers(x1,y1)&&covers(x1,y2)&&covers(x2,y2)&&covers(x2,y1);
	}
	
	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#intersects(com.vividsolutions.jts.geom.Geometry)
	 */
	@Override
	public boolean intersects(Geometry g) {
		// short-circuit test
		if (! getEnvelopeInternal().covers(g.getEnvelopeInternal()))
			return false;
		// optimization for rectangle arguments
		if (isRectangle()) {
			// since we have already tested that the test envelope is covered
			return true;
		}
		double x1,x2,y1,y2;
		x1=g.getEnvelopeInternal().getMinX();
		x2=g.getEnvelopeInternal().getMaxX();
		y1=g.getEnvelopeInternal().getMinY();
		y2=g.getEnvelopeInternal().getMaxY();	
		return covers(x1,y1)||covers(x1,y2)||covers(x2,y2)||covers(x2,y1);
	}
	
	/**
	 * Covers.
	 *
	 * @param x the x
	 * @param y the y
	 * @return true, if successful
	 */
	public boolean covers(double x, double y) {
		double distance = Math.sqrt((x-this.centerPoint.x)*(x-this.centerPoint.x)+(y-this.centerPoint.y)*(y-this.centerPoint.y));
		return distance<=this.radius?true:false;
	}
	 
	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#getGeometryType()
	 */
	@Override
	public String getGeometryType() {
		return "Circle";
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#getCoordinate()
	 */
	@Override
	public Coordinate getCoordinate() {
		return this.centerGeometry.getCoordinate();
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#getCoordinates()
	 */
	@Override
	public Coordinate[] getCoordinates() {
		return this.centerGeometry.getCoordinates();
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#getNumPoints()
	 */
	@Override
	public int getNumPoints() {
		return this.centerGeometry.getNumPoints();
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.centerGeometry.isEmpty();
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#getDimension()
	 */
	@Override
	public int getDimension() {
		return 0;
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#getBoundary()
	 */
	@Override
	public Geometry getBoundary() {
	    return getFactory().createGeometryCollection(null);
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#getBoundaryDimension()
	 */
	@Override
	public int getBoundaryDimension() {
		return 0;
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#reverse()
	 */
	@Override
	public Geometry reverse() {
		Geometry g = this.centerGeometry.reverse();
		Circle newCircle = new Circle(g,this.radius);
		return newCircle;
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#clone()
	 */
	@Override
	public Object clone() {
		Geometry g = (Geometry)this.centerGeometry.clone();
		Circle cloneCircle=new Circle(g,this.radius);
	    return cloneCircle;// return the clone
	}	
	
	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#equalsExact(com.vividsolutions.jts.geom.Geometry, double)
	 */
	@Override
	public boolean equalsExact(Geometry g, double tolerance) {
		String type1 = this.getGeometryType();
		String type2 = ((Geometry) g).getGeometryType();
		double radius1 = this.radius;
		double radius2 = ((Circle)g).radius;
		
		if(type1!=type2) return false;
		if(radius1 != radius2) return false;
		return this.centerGeometry.equals(((Circle)g).centerGeometry);
	}
	
	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#apply(com.vividsolutions.jts.geom.CoordinateFilter)
	 */
	@Override
	public void apply(CoordinateFilter filter) {
		// Do nothing. This circle is not expected to be a complete geometry.
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#apply(com.vividsolutions.jts.geom.CoordinateSequenceFilter)
	 */
	@Override
	public void apply(CoordinateSequenceFilter filter) {
		// Do nothing. This circle is not expected to be a complete geometry.		
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#apply(com.vividsolutions.jts.geom.GeometryFilter)
	 */
	@Override
	public void apply(GeometryFilter filter) {
		// Do nothing. This circle is not expected to be a complete geometry.		
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#apply(com.vividsolutions.jts.geom.GeometryComponentFilter)
	 */
	@Override
	public void apply(GeometryComponentFilter filter) {
		// Do nothing. This circle is not expected to be a complete geometry.
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#normalize()
	 */
	@Override
	public void normalize() {
		// Do nothing. This circle is not expected to be a complete geometry.
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#computeEnvelopeInternal()
	 */
	@Override
	protected Envelope computeEnvelopeInternal() {
	    if (isEmpty()) {
	        return new Envelope();
	      }
	      return this.MBR;
	}
	
	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#compareToSameClass(java.lang.Object)
	 */
	@Override
	protected int compareToSameClass(Object other) {
	    Envelope env = (Envelope) other;
	    Envelope mbr = this.MBR;
	    // compare based on numerical ordering of ordinates
	    if (mbr.getMinX() < env.getMinX()) return -1;
	    if (mbr.getMinX() > env.getMinX()) return 1;
	    if (mbr.getMinY() < env.getMinY()) return -1;
	    if (mbr.getMinY() > env.getMinY()) return 1;
	    if (mbr.getMaxX() < env.getMaxX()) return -1;
	    if (mbr.getMaxX() > env.getMaxX()) return 1;
	    if (mbr.getMaxY() < env.getMaxY()) return -1;
	    if (mbr.getMaxY() > env.getMaxY()) return 1;
	    return 0;
	}

	/* (non-Javadoc)
	 * @see com.vividsolutions.jts.geom.Geometry#compareToSameClass(java.lang.Object, com.vividsolutions.jts.geom.CoordinateSequenceComparator)
	 */
	@Override
	protected int compareToSameClass(Object other, CoordinateSequenceComparator comp) {
	    Envelope env = (Envelope) other;
	    Envelope mbr = this.MBR;
	    // compare based on numerical ordering of ordinates
	    if (mbr.getMinX() < env.getMinX()) return -1;
	    if (mbr.getMinX() > env.getMinX()) return 1;
	    if (mbr.getMinY() < env.getMinY()) return -1;
	    if (mbr.getMinY() > env.getMinY()) return 1;
	    if (mbr.getMaxX() < env.getMaxX()) return -1;
	    if (mbr.getMaxX() > env.getMaxX()) return 1;
	    if (mbr.getMaxY() < env.getMaxY()) return -1;
	    if (mbr.getMaxY() > env.getMaxY()) return 1;
	    return 0;
	}
}
