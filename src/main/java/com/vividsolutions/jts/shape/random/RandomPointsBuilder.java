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

package com.vividsolutions.jts.shape.random;

import com.vividsolutions.jts.algorithm.locate.IndexedPointInAreaLocator;
import com.vividsolutions.jts.algorithm.locate.PointOnGeometryLocator;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.shape.GeometricShapeBuilder;

/**
 * Creates random point sets contained in a 
 * region defined by either a rectangular or a polygonal extent. 
 * 
 * @author mbdavis
 *
 */
public class RandomPointsBuilder 
extends GeometricShapeBuilder
{
  protected Geometry maskPoly = null;
  private PointOnGeometryLocator extentLocator;

  /**
   * Create a shape factory which will create shapes using the default
   * {@link GeometryFactory}.
   */
  public RandomPointsBuilder()
  {
    super(new GeometryFactory());
  }

  /**
   * Create a shape factory which will create shapes using the given
   * {@link GeometryFactory}.
   *
   * @param geomFact the factory to use
   */
  public RandomPointsBuilder(GeometryFactory geomFact)
  {
  	super(geomFact);
  }

  /**
   * Sets a polygonal mask.
   * 
   * @param mask
   * @throws IllegalArgumentException if the mask is not polygonal
   */
  public void setExtent(Geometry mask)
  {
  	if (! (mask instanceof Polygonal))
  		throw new IllegalArgumentException("Only polygonal extents are supported");
  	this.maskPoly = mask;
  	setExtent(mask.getEnvelopeInternal());
  	extentLocator = new IndexedPointInAreaLocator(mask);
  }
  
  public Geometry getGeometry()
  {
  	Coordinate[] pts = new Coordinate[numPts];
  	int i = 0;
  	while (i < numPts) {
  		Coordinate p = createRandomCoord(getExtent());
  		if (extentLocator != null && ! isInExtent(p))
  			continue;
  		pts[i++] = p;
  	}
  	return geomFactory.createMultiPoint(pts);
  }
  
  protected boolean isInExtent(Coordinate p)
  {
  	if (extentLocator != null) 
  		return extentLocator.locate(p) != Location.EXTERIOR;
  	return getExtent().contains(p);
  }
  
  protected Coordinate createCoord(double x, double y)
  {
  	Coordinate pt = new Coordinate(x, y);
  	geomFactory.getPrecisionModel().makePrecise(pt);
    return pt;
  }
  
  protected Coordinate createRandomCoord(Envelope env)
  {
    double x = env.getMinX() + env.getWidth() * Math.random();
    double y = env.getMinY() + env.getHeight() * Math.random();
    return createCoord(x, y);
  }

}
