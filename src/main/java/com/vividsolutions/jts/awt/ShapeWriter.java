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
package com.vividsolutions.jts.awt;

import java.awt.Shape;
import java.awt.geom.GeneralPath;
import java.awt.geom.Point2D;
import java.util.ArrayList;

import com.vividsolutions.jts.geom.*;


/**
 * Writes {@link Geometry}s into Java2D {@link Shape} objects
 * of the appropriate type.
 * This supports rendering geometries using Java2D.
 * The ShapeWriter allows supplying a {@link PointTransformation}
 * class, to transform coordinates from model space into view space.
 * This is useful if a client is providing its own transformation
 * logic, rather than relying on Java2D <tt>AffineTransform</tt>s.
 * <p>
 * The writer supports removing duplicate consecutive points
 * (via the {@link #setRemoveDuplicatePoints(boolean)} method) 
 * as well as true <b>decimation</b>
 * (via the {@link #setDecimation(double)} method. 
 * Enabling one of these strategies can substantially improve 
 * rendering speed for large geometries.
 * It is only necessary to enable one strategy.
 * Using decimation is preferred, but this requires 
 * determining a distance below which input geometry vertices
 * can be considered unique (which may not always be feasible).
 * If neither strategy is enabled, all vertices
 * of the input <tt>Geometry</tt>
 * will be represented in the output <tt>Shape</tt>.
 * <p>
 * 
 */
public class ShapeWriter 
{
	/**
	 * The point transformation used by default.
	 */
	public static final PointTransformation DEFAULT_POINT_TRANSFORMATION = new IdentityPointTransformation();
	
	/**
	 * The point shape factory used by default.
	 */
	public static final PointShapeFactory DEFAULT_POINT_FACTORY = new PointShapeFactory.Square(3.0);
	
	private PointTransformation pointTransformer = DEFAULT_POINT_TRANSFORMATION;
	private PointShapeFactory pointFactory = DEFAULT_POINT_FACTORY;

	/**
	 * Cache a Point2D object to use to transfer coordinates into shape
	 */
	private Point2D transPoint = new Point2D.Double();

	/**
	 * If true, decimation will be used to reduce the number of vertices
	 * by removing consecutive duplicates.
	 * 
	 */
	private boolean doRemoveDuplicatePoints = false;
	
	private double decimationDistance = 0;
	
	/**
	 * Creates a new ShapeWriter with a specified point transformation
	 * and point shape factory.
	 * 
	 * @param pointTransformer a transformation from model to view space to use 
	 * @param pointFactory the PointShapeFactory to use
	 */
	public ShapeWriter(PointTransformation pointTransformer, PointShapeFactory pointFactory) 
	{
		if (pointTransformer != null)
			this.pointTransformer = pointTransformer;
		if (pointFactory != null)
			this.pointFactory = pointFactory;
	}

	/**
	 * Creates a new ShapeWriter with a specified point transformation
	 * and the default point shape factory.
	 * 
	 * @param pointTransformer a transformation from model to view space to use 
	 */
	public ShapeWriter(PointTransformation pointTransformer) 
	{
		this(pointTransformer, null);
	}

	/**
	 * Creates a new ShapeWriter with the default (identity) point transformation.
	 *
	 */
	public ShapeWriter() {
	}

	/**
	 * Sets whether duplicate consecutive points should be eliminated.
	 * This can reduce the size of the generated Shapes
	 * and improve rendering speed, especially in situations
	 * where a transform reduces the extent of the geometry.
	 * <p>
	 * The default is <tt>false</tt>.
	 * 
	 * @param doDecimation whether decimation is to be used
	 */
  public void setRemoveDuplicatePoints(boolean doRemoveDuplicatePoints)
  {
    this.doRemoveDuplicatePoints = doRemoveDuplicatePoints;
  }
  
  /**
   * Sets the decimation distance used to determine
   * whether vertices of the input geometry are 
   * considered to be duplicate and thus removed.
   * The distance is axis distance, not Euclidean distance.
   * The distance is specified in the input geometry coordinate system
   * (NOT the transformed output coordinate system).
   * <p>
   * When rendering to a screen image, a suitably small distance should be used
   * to avoid obvious rendering defects.  
   * A distance equivalent to the equivalent of 1.5 pixels or less is recommended
   * (and perhaps even smaller to avoid any chance of visible artifacts).
   * <p>
   * The default distance is 0.0, which disables decimation.
   * 
   * @param decimationDistance the distance below which vertices are considered to be duplicates
   */
  public void setDecimation(double decimationDistance)
  {
    this.decimationDistance = decimationDistance;
  }
  
	/**
	 * Creates a {@link Shape} representing a {@link Geometry}, 
	 * according to the specified PointTransformation
	 * and PointShapeFactory (if relevant).
	 * <p>
	 * Note that Shapes do not
	 * preserve information about which elements in heterogeneous collections
	 * are 1D and which are 2D.
	 * For example, a GeometryCollection containing a ring and a
	 * disk will render as two disks if Graphics.fill is used, 
	 * or as two rings if Graphics.draw is used.
	 * To avoid this issue use separate shapes for the components.
	 * 
	 * @param geometry the geometry to convert
	 * @return a Shape representing the geometry
	 */
	public Shape toShape(Geometry geometry)
	{
		if (geometry.isEmpty()) return new GeneralPath();
		if (geometry instanceof Polygon) return toShape((Polygon) geometry);
		if (geometry instanceof LineString) 			return toShape((LineString) geometry);
		if (geometry instanceof MultiLineString) 	return toShape((MultiLineString) geometry);
		if (geometry instanceof Point) 			return toShape((Point) geometry);
		if (geometry instanceof GeometryCollection) return toShape((GeometryCollection) geometry);

		throw new IllegalArgumentException(
			"Unrecognized Geometry class: " + geometry.getClass());
	}

	private Shape toShape(Polygon p) 
	{
		PolygonShape poly = new PolygonShape();
		
		appendRing(poly, p.getExteriorRing().getCoordinates());
		for (int j = 0; j < p.getNumInteriorRing(); j++) {
		  appendRing(poly, p.getInteriorRingN(j).getCoordinates());
		}

		return poly;
	}

	private void appendRing(PolygonShape poly, Coordinate[] coords) 
	{
    double prevx = Double.NaN;
    double prevy = Double.NaN;
    Coordinate prev = null;
    
    int n = coords.length - 1;
    /**
     * Don't include closing point.
     * Ring path will be closed explicitly, which provides a 
     * more accurate path representation.
     */
		for (int i = 0; i < n; i++) {
		  
		  if (decimationDistance > 0.0) {
		    boolean isDecimated = prev != null 
		      && Math.abs(coords[i].x - prev.x) < decimationDistance
		      && Math.abs(coords[i].y - prev.y) < decimationDistance;
		    if (i < n && isDecimated) 
		      continue;
		    prev = coords[i];
		  }
		  
			transformPoint(coords[i], transPoint);
			
			if (doRemoveDuplicatePoints) {
        // skip duplicate points (except the last point)
			  boolean isDup = transPoint.getX() == prevx && transPoint.getY() == prevy;
        if (i < n && isDup)
          continue;
        prevx = transPoint.getX();
        prevy = transPoint.getY();
			}
			poly.addToRing(transPoint);
		}
		// handle closing point
		poly.endRing();
	}
	
	private Shape toShape(GeometryCollection gc)
	{
		GeometryCollectionShape shape = new GeometryCollectionShape();
		// add components to GC shape
		for (int i = 0; i < gc.getNumGeometries(); i++) {
			Geometry g = (Geometry) gc.getGeometryN(i);
			shape.add(toShape(g));
		}
		return shape;
	}

	private GeneralPath toShape(MultiLineString mls)
	{
		GeneralPath path = new GeneralPath();

		for (int i = 0; i < mls.getNumGeometries(); i++) {
			LineString lineString = (LineString) mls.getGeometryN(i);
			path.append(toShape(lineString), false);
		}
		return path;
	}

	private GeneralPath toShape(LineString lineString)
	{
		GeneralPath shape = new GeneralPath();
		
    Coordinate prev = lineString.getCoordinateN(0);
    transformPoint(prev, transPoint);
		shape.moveTo((float) transPoint.getX(), (float) transPoint.getY());

    double prevx = (double) transPoint.getX();
    double prevy = (double) transPoint.getY();
    
    int n = lineString.getNumPoints() - 1;
    //int count = 0;
    for (int i = 1; i <= n; i++) {
      Coordinate currentCoord = lineString.getCoordinateN(i);
      if (decimationDistance > 0.0) {
        boolean isDecimated = prev != null
            && Math.abs(currentCoord.x - prev.x) < decimationDistance
            && Math.abs(currentCoord.y - prev.y) < decimationDistance;
        if (i < n && isDecimated) {
          continue;
        }
        prev = currentCoord;
      }

      transformPoint(currentCoord, transPoint);

			if (doRemoveDuplicatePoints) {
  			// skip duplicate points (except the last point)
			  boolean isDup = transPoint.getX() == prevx && transPoint.getY() == prevy;
  			if (i < n && isDup)
  			  continue;
  			prevx = transPoint.getX();
  			prevy = transPoint.getY();
  			//count++;
			}
			shape.lineTo((float) transPoint.getX(), (float) transPoint.getY());
		}
		//System.out.println(count);
		return shape;
	}

	private Shape toShape(Point point)
  {
		Point2D viewPoint = transformPoint(point.getCoordinate());
		return pointFactory.createPoint(viewPoint);
	}

  private Point2D transformPoint(Coordinate model) {
		return transformPoint(model, new Point2D.Double());
	}
  
  private Point2D transformPoint(Coordinate model, Point2D view) {
		pointTransformer.transform(model, view);
		return view;
	}
}
