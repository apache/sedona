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

import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.algorithm.locate.IndexedPointInAreaLocator;
import com.vividsolutions.jts.algorithm.locate.PointOnGeometryLocator;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.math.MathUtil;
import com.vividsolutions.jts.shape.GeometricShapeBuilder;

/**
 * Creates random point sets 
 * where the points are constrained to lie in the cells of a grid. 
 * 
 * @author mbdavis
 *
 */
public class RandomPointsInGridBuilder 
extends GeometricShapeBuilder
{
	private boolean isConstrainedToCircle = false;
	private double gutterFraction = 0;
	
  /**
   * Create a builder which will create shapes using the default
   * {@link GeometryFactory}.
   */
  public RandomPointsInGridBuilder()
  {
    super(new GeometryFactory());
  }

  /**
   * Create a builder which will create shapes using the given
   * {@link GeometryFactory}.
   *
   * @param geomFact the factory to use
   */
  public RandomPointsInGridBuilder(GeometryFactory geomFact)
  {
  	super(geomFact);
  }

  /**
   * Sets whether generated points are constrained to lie
   * within a circle contained within each grid cell.
   * This provides greater separation between points
   * in adjacent cells.
   * <p>
   * The default is to not be constrained to a circle.
   * @param isConstrainedToCircle
   */
  public void setConstrainedToCircle(boolean isConstrainedToCircle)
  {
  	this.isConstrainedToCircle = isConstrainedToCircle;
  }
  
  /**
   * Sets the fraction of the grid cell side which will be treated as
   * a gutter, in which no points will be created.
   * The provided value is clamped to the range [0.0, 1.0].
   * 
   * @param gutterFraction
   */
  public void setGutterFraction(double gutterFraction)
  {
  	this.gutterFraction = gutterFraction;
  }
  
  /**
   * Gets the {@link MultiPoint} containing the generated point
   * 
   * @return a MultiPoint
   */
  public Geometry getGeometry()
  {
    int nCells = (int) Math.sqrt(numPts);
    // ensure that at least numPts points are generated
    if (nCells * nCells < numPts)
      nCells += 1;

    double gridDX = getExtent().getWidth() / nCells;
    double gridDY = getExtent().getHeight() / nCells;

    double gutterFrac = MathUtil.clamp(gutterFraction, 0.0, 1.0);
    double gutterOffsetX = gridDX * gutterFrac/2;
    double gutterOffsetY = gridDY * gutterFrac/2;
    double cellFrac = 1.0 - gutterFrac;
    double cellDX = cellFrac * gridDX;
    double cellDY = cellFrac * gridDY;
    	
    Coordinate[] pts = new Coordinate[nCells * nCells];
    int index = 0;
    for (int i = 0; i < nCells; i++) {
      for (int j = 0; j < nCells; j++) {
      	double orgX = getExtent().getMinX() + i * gridDX + gutterOffsetX;
      	double orgY = getExtent().getMinY() + j * gridDY + gutterOffsetY;
        pts[index++] = randomPointInCell(orgX, orgY, cellDX, cellDY);
      }
    }
    return geomFactory.createMultiPoint(pts);
  }
  
  private Coordinate randomPointInCell(double orgX, double orgY, double xLen, double yLen)
  {
  	if (isConstrainedToCircle) {
  		return randomPointInCircle(
  				orgX, 
  				orgY, 
  				xLen, yLen);
  	}
  	return randomPointInGridCell(orgX, orgY, xLen, yLen);
  }
  
  private Coordinate randomPointInGridCell(double orgX, double orgY, double xLen, double yLen)
  {
    double x = orgX + xLen * Math.random();
    double y = orgY + yLen * Math.random();
    return createCoord(x, y);
  }

  private static Coordinate randomPointInCircle(double orgX, double orgY, double width, double height)
  {
  	double centreX = orgX + width/2;
  	double centreY = orgY + height/2;
  		
  	double rndAng = 2 * Math.PI * Math.random();
  	double rndRadius = Math.random();
    // use square root of radius, since area is proportional to square of radius
    double rndRadius2 = Math.sqrt(rndRadius);
  	double rndX = width/2 * rndRadius2 * Math.cos(rndAng); 
  	double rndY = height/2 * rndRadius2 * Math.sin(rndAng); 
  	
    double x0 = centreX + rndX;
    double y0 = centreY + rndY;
    return new Coordinate(x0, y0);    
  }

}
