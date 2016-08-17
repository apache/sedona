/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package com.vividsolutions.jts.util;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.util.AffineTransformation;

/**
 * Computes various kinds of common geometric shapes.
 * Provides various ways of specifying the location and extent
 * and rotations of the generated shapes,
 * as well as number of line segments used to form them.
 * <p>
 * <b>Example of usage:</b>
 * <pre>
 *  GeometricShapeFactory gsf = new GeometricShapeFactory();
 *  gsf.setSize(100);
 *  gsf.setNumPoints(100);
 *  gsf.setBase(new Coordinate(100, 100));
 *  gsf.setRotation(0.5);
 *  Polygon rect = gsf.createRectangle();
 * </pre>
 *
 * @version 1.7
 */
public class GeometricShapeFactory
{
  protected GeometryFactory geomFact;
  protected PrecisionModel precModel = null;
  protected Dimensions dim = new Dimensions();
  protected int nPts = 100;
  
  /**
   * Default is no rotation.
   */
  protected double rotationAngle = 0.0;

  /**
   * Create a shape factory which will create shapes using the default
   * {@link GeometryFactory}.
   */
  public GeometricShapeFactory()
  {
    this(new GeometryFactory());
  }

  /**
   * Create a shape factory which will create shapes using the given
   * {@link GeometryFactory}.
   *
   * @param geomFact the factory to use
   */
  public GeometricShapeFactory(GeometryFactory geomFact)
  {
    this.geomFact = geomFact;
    precModel = geomFact.getPrecisionModel();
  }

  public void setEnvelope(Envelope env)
  {
  	dim.setEnvelope(env);
  }
  
  /**
   * Sets the location of the shape by specifying the base coordinate
   * (which in most cases is the
   * lower left point of the envelope containing the shape).
   *
   * @param base the base coordinate of the shape
   */
  public void setBase(Coordinate base)  {  dim.setBase(base);    }
  /**
   * Sets the location of the shape by specifying the centre of
   * the shape's bounding box
   *
   * @param centre the centre coordinate of the shape
   */
  public void setCentre(Coordinate centre)  {  dim.setCentre(centre);    }

  /**
   * Sets the total number of points in the created {@link Geometry}.
   * The created geometry will have no more than this number of points,
   * unless more are needed to create a valid geometry.
   */
  public void setNumPoints(int nPts) { this.nPts = nPts; }

  /**
   * Sets the size of the extent of the shape in both x and y directions.
   *
   * @param size the size of the shape's extent
   */
  public void setSize(double size) { dim.setSize(size); }

  /**
   * Sets the width of the shape.
   *
   * @param width the width of the shape
   */
  public void setWidth(double width) { dim.setWidth(width); }

  /**
   * Sets the height of the shape.
   *
   * @param height the height of the shape
   */
  public void setHeight(double height) { dim.setHeight(height); }

  /**
   * Sets the rotation angle to use for the shape.
   * The rotation is applied relative to the centre of the shape.
   * 
   * @param radians the rotation angle in radians.
   */
  public void setRotation(double radians)
  {
    rotationAngle = radians;
  }
  
  protected Geometry rotate(Geometry geom)
  {
    if (rotationAngle != 0.0) {
      AffineTransformation trans = AffineTransformation.rotationInstance(rotationAngle, 
          dim.getCentre().x, dim.getCentre().y);
      geom.apply(trans);
    }
    return geom;
  }
  
  /**
   * Creates a rectangular {@link Polygon}.
   *
   * @return a rectangular Polygon
   *
   */
  public Polygon createRectangle()
  {
    int i;
    int ipt = 0;
    int nSide = nPts / 4;
    if (nSide < 1) nSide = 1;
    double XsegLen = dim.getEnvelope().getWidth() / nSide;
    double YsegLen = dim.getEnvelope().getHeight() / nSide;

    Coordinate[] pts = new Coordinate[4 * nSide + 1];
    Envelope env = dim.getEnvelope();

    //double maxx = env.getMinX() + nSide * XsegLen;
    //double maxy = env.getMinY() + nSide * XsegLen;

    for (i = 0; i < nSide; i++) {
      double x = env.getMinX() + i * XsegLen;
      double y = env.getMinY();
      pts[ipt++] = coord(x, y);
    }
    for (i = 0; i < nSide; i++) {
      double x = env.getMaxX();
      double y = env.getMinY() + i * YsegLen;
      pts[ipt++] = coord(x, y);
    }
    for (i = 0; i < nSide; i++) {
      double x = env.getMaxX() - i * XsegLen;
      double y = env.getMaxY();
      pts[ipt++] = coord(x, y);
    }
    for (i = 0; i < nSide; i++) {
      double x = env.getMinX();
      double y = env.getMaxY() - i * YsegLen;
      pts[ipt++] = coord(x, y);
    }
    pts[ipt++] = new Coordinate(pts[0]);

    LinearRing ring = geomFact.createLinearRing(pts);
    Polygon poly = geomFact.createPolygon(ring, null);
    return (Polygon) rotate(poly);
  }

//* @deprecated use {@link createEllipse} instead
  /**
   * Creates a circular or elliptical {@link Polygon}.
   *
   * @return a circle or ellipse
   */
  public Polygon createCircle()
  {
    return createEllipse();
  }
  
  /**
   * Creates an elliptical {@link Polygon}.
   * If the supplied envelope is square the 
   * result will be a circle. 
   *
   * @return an ellipse or circle
   */
  public Polygon createEllipse()
  {

    Envelope env = dim.getEnvelope();
    double xRadius = env.getWidth() / 2.0;
    double yRadius = env.getHeight() / 2.0;

    double centreX = env.getMinX() + xRadius;
    double centreY = env.getMinY() + yRadius;

    Coordinate[] pts = new Coordinate[nPts + 1];
    int iPt = 0;
    for (int i = 0; i < nPts; i++) {
        double ang = i * (2 * Math.PI / nPts);
        double x = xRadius * Math.cos(ang) + centreX;
        double y = yRadius * Math.sin(ang) + centreY;
        pts[iPt++] = coord(x, y);
    }
    pts[iPt] = new Coordinate(pts[0]);

    LinearRing ring = geomFact.createLinearRing(pts);
    Polygon poly = geomFact.createPolygon(ring, null);
    return (Polygon) rotate(poly);
  }
  /**
   * Creates a squircular {@link Polygon}.
   *
   * @return a squircle
   */
  public Polygon createSquircle()
  /**
   * Creates a squircular {@link Polygon}.
   *
   * @return a squircle
   */
  {
  	return createSupercircle(4);
  }
  
  /**
   * Creates a supercircular {@link Polygon}
   * of a given positive power.
   *
   * @return a supercircle
   */
  public Polygon createSupercircle(double power)
  {
  	double recipPow = 1.0 / power;
  	
    double radius = dim.getMinSize() / 2;
    Coordinate centre = dim.getCentre();
    
    double r4 = Math.pow(radius, power);
    double y0 = radius;
    
    double xyInt = Math.pow(r4 / 2, recipPow);
    
    int nSegsInOct = nPts / 8;
    int totPts = nSegsInOct * 8 + 1;
    Coordinate[] pts = new Coordinate[totPts];
    double xInc = xyInt / nSegsInOct;
    
    for (int i = 0; i <= nSegsInOct; i++) {
  		double x = 0.0;
  		double y = y0;
    	if (i != 0) {
    		x = xInc * i;
    		double x4 = Math.pow(x, power);
    		y = Math.pow(r4 - x4, recipPow);
    	}
      pts[i] = coordTrans(x, y, centre);
      pts[2 * nSegsInOct - i] = coordTrans(y, x, centre);
      
      pts[2 * nSegsInOct + i] = coordTrans(y, -x, centre);
      pts[4 * nSegsInOct - i] = coordTrans(x, -y, centre);
      
      pts[4 * nSegsInOct + i] = coordTrans(-x, -y, centre);
      pts[6 * nSegsInOct - i] = coordTrans(-y, -x, centre);
      
      pts[6 * nSegsInOct + i] = coordTrans(-y, x, centre);
      pts[8 * nSegsInOct - i] = coordTrans(-x, y, centre);
    }
    pts[pts.length-1] = new Coordinate(pts[0]);

    LinearRing ring = geomFact.createLinearRing(pts);
    Polygon poly = geomFact.createPolygon(ring, null);
    return (Polygon) rotate(poly);
  }

   /**
    * Creates an elliptical arc, as a {@link LineString}.
    * The arc is always created in a counter-clockwise direction.
    * This can easily be reversed if required by using 
    * {#link LineString.reverse()}
    *
    * @param startAng start angle in radians
    * @param angExtent size of angle in radians
    * @return an elliptical arc
    */
  public LineString createArc(
     double startAng,
     double angExtent)
  {
    Envelope env = dim.getEnvelope();
    double xRadius = env.getWidth() / 2.0;
    double yRadius = env.getHeight() / 2.0;

    double centreX = env.getMinX() + xRadius;
    double centreY = env.getMinY() + yRadius;

     double angSize = angExtent;
     if (angSize <= 0.0 || angSize > 2 * Math.PI)
       angSize = 2 * Math.PI;
     double angInc = angSize / (nPts - 1);

     Coordinate[] pts = new Coordinate[nPts];
     int iPt = 0;
     for (int i = 0; i < nPts; i++) {
         double ang = startAng + i * angInc;
         double x = xRadius * Math.cos(ang) + centreX;
         double y = yRadius * Math.sin(ang) + centreY;
         pts[iPt++] = coord(x, y);
     }
     LineString line = geomFact.createLineString(pts);
     return (LineString) rotate(line);
   }

  /**
   * Creates an elliptical arc polygon.
   * The polygon is formed from the specified arc of an ellipse
   * and the two radii connecting the endpoints to the centre of the ellipse.
   *
   * @param startAng start angle in radians
   * @param angExtent size of angle in radians
   * @return an elliptical arc polygon
   */
  public Polygon createArcPolygon(double startAng, double angExtent) {
    Envelope env = dim.getEnvelope();
    double xRadius = env.getWidth() / 2.0;
    double yRadius = env.getHeight() / 2.0;

    double centreX = env.getMinX() + xRadius;
    double centreY = env.getMinY() + yRadius;

    double angSize = angExtent;
    if (angSize <= 0.0 || angSize > 2 * Math.PI)
      angSize = 2 * Math.PI;
    double angInc = angSize / (nPts - 1);
    // double check = angInc * nPts;
    // double checkEndAng = startAng + check;

    Coordinate[] pts = new Coordinate[nPts + 2];

    int iPt = 0;
    pts[iPt++] = coord(centreX, centreY);
    for (int i = 0; i < nPts; i++) {
      double ang = startAng + angInc * i;

      double x = xRadius * Math.cos(ang) + centreX;
      double y = yRadius * Math.sin(ang) + centreY;
      pts[iPt++] = coord(x, y);
    }
    pts[iPt++] = coord(centreX, centreY);
    LinearRing ring = geomFact.createLinearRing(pts);
    Polygon poly = geomFact.createPolygon(ring, null);
    return (Polygon) rotate(poly);
  }

  protected Coordinate coord(double x, double y)
  {
  	Coordinate pt = new Coordinate(x, y);
    precModel.makePrecise(pt);
    return pt;
  }
  
  protected Coordinate coordTrans(double x, double y, Coordinate trans)
  {
  	return coord(x + trans.x, y + trans.y);
  }
  
  protected class Dimensions
  {
    public Coordinate base;
    public Coordinate centre;
    public double width;
    public double height;

    public void setBase(Coordinate base)  {  this.base = base;    }
    public Coordinate getBase() { return base; }
    
    public void setCentre(Coordinate centre)  {  this.centre = centre;    }
    public Coordinate getCentre() 
    { 
      if (centre == null) {
        centre = new Coordinate(base.x + width/2, base.y + height/2);
      }
      return centre; 
    }
   
    public void setSize(double size)
    {
      height = size;
      width = size;
    }

    public double getMinSize()
    {
    	return Math.min(width, height);
    }
    public void setWidth(double width) { this.width = width; }
    public double getWidth() { return width; }
    public double getHeight() { return height; }
    
    public void setHeight(double height) { this.height = height; }

    public void setEnvelope(Envelope env)
    {
    	this.width = env.getWidth();
    	this.height = env.getHeight();
    	this.base = new Coordinate(env.getMinX(), env.getMinY());
    	this.centre = new Coordinate(env.centre());
    }
    
    public Envelope getEnvelope() {
      if (base != null) {
        return new Envelope(base.x, base.x + width, base.y, base.y + height);
      }
      if (centre != null) {
        return new Envelope(centre.x - width/2, centre.x + width/2,
                            centre.y - height/2, centre.y + height/2);
      }
      return new Envelope(0, width, 0, height);
    }
    
  }
}
